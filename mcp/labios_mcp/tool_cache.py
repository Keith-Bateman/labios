"""Tool-call cache for the LABIOS MCP frontend.

Ported from SWE-bench's agent/tool_cache.py tool-call-caching design
(/u/kbateman/.claude/plans/dapper-cooking-lampson.md), adapted to labios's own
vocabulary. Cache entries are read/written as ordinary labios labels against a
``clio://`` destination -- the same ``client.create_label``/``publish``/
``wait_all`` path every other MCP call already uses (see
/u/kbateman/.claude/plans/dazzling-leaping-sprout.md's "Decision" section for
why this reuses labios's own clio-core backend instead of a second, direct
CTE client the way SWE-bench's ClioCTEBackend did).

Cache scope is per MCP server process (one ``stdio_server()`` per client
connection, see ``labios_mcp/server.py:main``), the same role
``f"{run_id}__{instance_id}"`` played in SWE-bench -- no cross-connection
sharing.

Invalidation is sequence-based, not TTL-based: a per-process
``uri -> write sequence number`` index, bumped by every write. A cached
entry's dependency snapshot must still match current sequence numbers for a
hit to count.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, wait as wait_futures
from dataclasses import dataclass
from typing import Any, Optional

import labios

TOOLCACHE_MAX_BYTES = int(os.environ.get("LABIOS_MCP_TOOLCACHE_MAX_BYTES", "8000000"))
CACHE_OP_TIMEOUT_MS = int(os.environ.get("LABIOS_MCP_TOOLCACHE_OP_TIMEOUT_MS", "2000"))

# The one shared "full raw content of this source URI" cache entry, consulted
# and populated by both retrieve() and process()'s safe-inline-op fast path --
# the direct analogue of SWE-bench's _get_or_read_raw sharing one entry across
# read_file/grep/bash cat. Never a real tool name, so it can't collide with an
# actual (tool, args) cache entry.
RAW_SOURCE_NS = "__raw_source__"

# Pipeline operations whose C++ implementation (src/labios/sds/builtins.cpp)
# is deterministic, RNG-free, and simple enough to safely reimplement
# byte-exact in Python -- see apply_safe_stage. builtin://sample is
# deliberately excluded: it draws from a seeded std::mt19937 /
# std::uniform_int_distribution whose exact output is not portably
# reproducible from Python without a bit-exact reimplementation of
# libstdc++'s PRNG/distribution algorithms.
SAFE_INLINE_OPS = frozenset({
    "builtin://filter_bytes",
    "builtin://truncate",
    "builtin://deduplicate",
})

_SIGNED_DECIMAL = re.compile(r"^-?\d+$")
_UNSIGNED_DECIMAL = re.compile(r"^\d+$")


def apply_safe_stage(operation: str, args: str, data: bytes) -> bytes:
    """Byte-exact Python reimplementation of a SAFE_INLINE_OPS builtin.

    Mirrors src/labios/sds/builtins.cpp's fn_filter_bytes/fn_truncate/
    fn_deduplicate exactly, including their C++ parsing quirks (filter_bytes'
    pattern byte is `static_cast<std::byte>(int)`, which wraps mod 256 for
    out-of-range/negative input; truncate's N is parsed as an unsigned
    size_t, so a leading '-' is invalid, matching std::from_chars there).
    Raises ValueError on anything not provably byte-exact-safe -- callers
    must fall back to a real labios_process call, never surface this as a
    user-facing error.
    """
    if operation == "builtin://filter_bytes":
        if not args or not _SIGNED_DECIMAL.match(args):
            raise ValueError("filter_bytes: invalid pattern byte")
        pattern = int(args) & 0xFF
        return bytes(b for b in data if b == pattern)
    if operation == "builtin://truncate":
        if not args or not _UNSIGNED_DECIMAL.match(args):
            raise ValueError("truncate: invalid N")
        n = int(args)
        return data[:n]
    if operation == "builtin://deduplicate":
        if not data:
            return b""
        out = bytearray([data[0]])
        for b in data[1:]:
            if b != out[-1]:
                out.append(b)
        return bytes(out)
    raise ValueError(f"{operation} is not a safe inline op")


def canonical_key(tool_name: str, args: dict[str, Any]) -> str:
    return json.dumps({"tool": tool_name, "args": args}, sort_keys=True)


def blob_name_for(tool_name: str, args: dict[str, Any]) -> str:
    digest = hashlib.sha256(canonical_key(tool_name, args).encode("utf-8")).hexdigest()
    return f"v1/{digest}"


class Backend:
    """Minimal get/put/close surface a ToolCache needs."""

    def get(self, tag: str, blob: str) -> Optional[bytes]:
        raise NotImplementedError

    def put(self, tag: str, blob: str, data: bytes) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class NullBackend(Backend):
    """Always-miss, no-op backend -- used when caching is disabled
    (LABIOS_MCP_TOOL_CACHE=0) and as McpFrontend's default so constructing
    one without an explicit cache (every existing hermetic test does this)
    never touches the client at all."""

    def get(self, tag: str, blob: str) -> Optional[bytes]:
        return None

    def put(self, tag: str, blob: str, data: bytes) -> None:
        pass


def _state_name(waited: Any) -> str:
    value = getattr(waited, "state", "unknown")
    return str(getattr(value, "name", value)).lower()


class ClioLabelBackend(Backend):
    """Reads/writes cache entries as ordinary labios labels against a
    ``clio://<tag>/<blob>`` URI, through the same client the rest of the MCP
    frontend already uses. Any non-"complete" outcome (clio_enabled=false on
    the worker, timeout, parked, failed) is treated as a miss, never raised --
    a broken or absent cache must never fail the real tool call it backs.
    put() is backgrounded (a lost/slow store just means a future miss, same
    reasoning as the SWE-bench source material) via a small thread pool;
    unlike SWE-bench's ClioCTEBackend this needs no SIGALRM/thread-affinity
    handling, since labios.Client's own bindings already release the GIL and
    wait_all() is already a bounded wait.
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        self._put_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="labios-cache-put")
        self._pending: list = []
        self._pending_lock = threading.Lock()

    @staticmethod
    def _uri(tag: str, blob: str) -> str:
        return f"clio://{tag}/{blob}"

    def wait_for_pending_puts(self, timeout: Optional[float] = None) -> None:
        """Block until every put() submitted so far has finished. Only for
        tests/shutdown -- puts are deliberately fire-and-forget on the real
        request-serving path (see class docstring)."""
        with self._pending_lock:
            pending, self._pending = self._pending, []
        wait_futures(pending, timeout=timeout)

    def get(self, tag: str, blob: str) -> Optional[bytes]:
        try:
            params = labios.LabelParams()
            params.type = labios.LabelType.Read
            params.source_resource = labios.resource_from_uri(self._uri(tag, blob))
            params.intent = labios.Intent.CACHE
            label = self._client.create_label(params)
            operation = self._client.publish(label)
            waited = operation.wait_all(CACHE_OP_TIMEOUT_MS)
            if _state_name(waited) != "complete":
                return None
            return bytes(operation.read(0))
        except Exception:
            return None

    def put(self, tag: str, blob: str, data: bytes) -> None:
        def _do_put() -> None:
            try:
                params = labios.LabelParams()
                params.type = labios.LabelType.Write
                params.destination_resource = labios.resource_from_uri(self._uri(tag, blob))
                params.intent = labios.Intent.CACHE
                label = self._client.create_label(params)
                operation = self._client.publish(label, data)
                operation.wait_all(CACHE_OP_TIMEOUT_MS)
            except Exception:
                pass  # best-effort: a lost/failed store just means a future get() misses

        future = self._put_executor.submit(_do_put)
        with self._pending_lock:
            self._pending.append(future)

    def close(self) -> None:
        self._put_executor.shutdown(wait=False, cancel_futures=False)


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    stores: int = 0
    invalidated: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "hits": self.hits, "misses": self.misses,
            "stores": self.stores, "invalidated": self.invalidated,
        }


class ToolCache:
    def __init__(self, backend: Backend, session_id: str) -> None:
        self.backend = backend
        self.session_id = session_id
        self.tag = f"mcp-toolcache-{session_id}"
        self.write_seq: dict[str, int] = {}
        self.stats = CacheStats()

    def close(self) -> None:
        self.backend.close()

    # -- invalidation index --------------------------------------------------

    def record_write(self, uris: list[str]) -> None:
        for u in uris:
            self.write_seq[u] = self.write_seq.get(u, 0) + 1
        self.stats.invalidated += len(uris)

    def _deps_still_valid(self, deps: dict[str, int]) -> bool:
        return all(self.write_seq.get(uri, 0) == seq for uri, seq in deps.items())

    def _current_deps(self, uris: list[str]) -> dict[str, int]:
        return {u: self.write_seq.get(u, 0) for u in uris}

    # -- get/put --------------------------------------------------------------

    def get(self, tool_name: str, args: dict[str, Any]) -> Optional[Any]:
        blob = blob_name_for(tool_name, args)
        raw = self.backend.get(self.tag, blob)
        if raw is None:
            self.stats.misses += 1
            return None
        try:
            record = json.loads(raw.decode("utf-8"))
        except Exception:
            self.stats.misses += 1
            return None
        if not self._deps_still_valid(record.get("deps", {})):
            self.stats.misses += 1
            return None
        self.stats.hits += 1
        return record["content"]

    def put(self, tool_name: str, args: dict[str, Any], content: Any, dep_uris: list[str]) -> None:
        blob = blob_name_for(tool_name, args)
        record = {"deps": self._current_deps(dep_uris), "content": content}
        payload = json.dumps(record).encode("utf-8")
        if len(payload) > TOOLCACHE_MAX_BYTES:
            return
        self.backend.put(self.tag, blob, payload)
        self.stats.stores += 1

    # -- shared raw-source entry ("tool transformation") ---------------------
    #
    # One entry per distinct source URI, shared between retrieve() (full
    # reads only, size==0) and process()'s safe-inline-op fast path -- the
    # direct analogue of SWE-bench's decompose_bash + _get_or_read_raw
    # sharing one cache entry across differently-shaped tool calls that touch
    # the same underlying content.

    def get_raw_source(self, source: str) -> Optional[bytes]:
        content = self.get(RAW_SOURCE_NS, {"source": source})
        if content is None:
            return None
        return base64.b64decode(content)

    def put_raw_source(self, source: str, data: bytes) -> None:
        self.put(RAW_SOURCE_NS, {"source": source}, base64.b64encode(data).decode("ascii"), [source])
