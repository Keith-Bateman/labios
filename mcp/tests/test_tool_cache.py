"""Hermetic unit tests for labios_mcp.tool_cache, independent of a real or
fake labios.Client -- see test_server.py for the ClioLabelBackend/FakeClient
round-trip tests exercising this module through McpFrontend."""
from __future__ import annotations

from typing import Optional

import pytest

from labios_mcp.tool_cache import (
    SAFE_INLINE_OPS,
    Backend,
    NullBackend,
    ToolCache,
    apply_safe_stage,
    blob_name_for,
    canonical_key,
)


class DictBackend(Backend):
    """In-memory Backend for testing ToolCache's own logic (key
    construction, invalidation) without any real or fake labios client."""

    def __init__(self) -> None:
        self.store: dict[tuple[str, str], bytes] = {}

    def get(self, tag: str, blob: str) -> Optional[bytes]:
        return self.store.get((tag, blob))

    def put(self, tag: str, blob: str, data: bytes) -> None:
        self.store[(tag, blob)] = data


def test_canonical_key_is_order_independent_and_blob_name_is_deterministic():
    a = canonical_key("labios_retrieve", {"source": "file:///x", "size": 0})
    b = canonical_key("labios_retrieve", {"size": 0, "source": "file:///x"})
    assert a == b
    assert blob_name_for("labios_retrieve", {"source": "file:///x", "size": 0}) == \
        blob_name_for("labios_retrieve", {"size": 0, "source": "file:///x"})
    assert blob_name_for("labios_retrieve", {"source": "file:///x", "size": 0}) != \
        blob_name_for("labios_retrieve", {"source": "file:///y", "size": 0})


def test_null_backend_always_misses_and_put_is_a_no_op():
    backend = NullBackend()
    backend.put("tag", "blob", b"data")
    assert backend.get("tag", "blob") is None


def test_toolcache_hit_then_miss_after_write_invalidation():
    cache = ToolCache(DictBackend(), session_id="unit")
    assert cache.get("labios_retrieve", {"source": "file:///a"}) is None
    assert cache.stats.misses == 1

    cache.put("labios_retrieve", {"source": "file:///a"}, "content", ["file:///a"])
    assert cache.stats.stores == 1
    assert cache.get("labios_retrieve", {"source": "file:///a"}) == "content"
    assert cache.stats.hits == 1

    cache.record_write(["file:///a"])
    assert cache.get("labios_retrieve", {"source": "file:///a"}) is None
    assert cache.stats.misses == 2
    assert cache.stats.invalidated == 1


def test_toolcache_write_to_unrelated_uri_does_not_invalidate():
    cache = ToolCache(DictBackend(), session_id="unit")
    cache.put("labios_retrieve", {"source": "file:///a"}, "content", ["file:///a"])
    cache.record_write(["file:///b"])
    assert cache.get("labios_retrieve", {"source": "file:///a"}) == "content"


def test_raw_source_cache_round_trips_and_is_keyed_only_by_source():
    cache = ToolCache(DictBackend(), session_id="unit")
    assert cache.get_raw_source("file:///a") is None
    cache.put_raw_source("file:///a", b"\x00raw\xff")
    assert cache.get_raw_source("file:///a") == b"\x00raw\xff"
    # A write to the source invalidates the shared entry too.
    cache.record_write(["file:///a"])
    assert cache.get_raw_source("file:///a") is None


@pytest.mark.parametrize("data,pattern,expected", [
    (b"", "65", b""),
    (b"AAB", "65", b"AA"),          # 65 == ord('A')
    (b"AAB", "66", b"B"),           # 66 == ord('B')
    (b"AAB", "-191", b"AA"),        # -191 & 0xFF == 65, mirrors the C++
                                    # static_cast<std::byte>(int) wraparound
])
def test_apply_safe_stage_filter_bytes_matches_cxx_semantics(data, pattern, expected):
    assert apply_safe_stage("builtin://filter_bytes", pattern, data) == expected


def test_apply_safe_stage_filter_bytes_rejects_non_decimal_args():
    with pytest.raises(ValueError):
        apply_safe_stage("builtin://filter_bytes", "+65", b"AAB")
    with pytest.raises(ValueError):
        apply_safe_stage("builtin://filter_bytes", "", b"AAB")


@pytest.mark.parametrize("data,n,expected", [
    (b"", "0", b""),
    (b"hello world", "5", b"hello"),
    (b"hi", "10", b"hi"),  # N > len(data) -- same as std::min(n, input.size())
])
def test_apply_safe_stage_truncate_matches_cxx_semantics(data, n, expected):
    assert apply_safe_stage("builtin://truncate", n, data) == expected


def test_apply_safe_stage_truncate_rejects_negative_args():
    with pytest.raises(ValueError):
        apply_safe_stage("builtin://truncate", "-1", b"hello")


@pytest.mark.parametrize("data,expected", [
    (b"", b""),
    (b"aaabccca", b"abca"),
    (b"abc", b"abc"),
])
def test_apply_safe_stage_deduplicate_matches_cxx_semantics(data, expected):
    assert apply_safe_stage("builtin://deduplicate", "", data) == expected


def test_sample_is_not_a_safe_inline_op():
    # builtin://sample draws from a seeded std::mt19937/
    # std::uniform_int_distribution -- not portably reproducible from Python,
    # so it must never be treated as safe to reimplement inline.
    assert "builtin://sample" not in SAFE_INLINE_OPS
    with pytest.raises(ValueError):
        apply_safe_stage("builtin://sample", "3", b"abcdef")


def test_unlisted_operation_is_rejected():
    with pytest.raises(ValueError):
        apply_safe_stage("builtin://compress_rle", "", b"abc")
