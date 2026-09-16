"""Hermetic contract tests for MCP lowering through the public Python SDK."""
from __future__ import annotations

import base64
from enum import Enum
import inspect
from pathlib import Path
from types import SimpleNamespace

import flatbuffers
import pytest

import labios
from labios import MalformedRegistryBuffer, UnsupportedRegistryVersion
from labios.registry import Payload, PayloadKind
from labios.registry import WorkerDescriptor as FbWorker
from labios.registry import WorkerRegistryMessage as FbMessage
from labios.registry import WorkerRegistrySnapshot as FbSnapshot
from labios_mcp.server import (
    McpFrontend,
    decode_registry_snapshot,
    list_tools,
)
from labios_mcp.tool_cache import ClioLabelBackend, NullBackend, ToolCache


class State(Enum):
    PENDING = 0
    COMPLETE = 1
    FAILED = 2
    CANCELLED = 3
    PARKED = 4
    TIMEOUT = 5
    UNKNOWN = 6


class Cancellation(Enum):
    CANCELLED = 0
    TOO_LATE = 1
    TERMINAL = 2
    UNKNOWN = 3


def completion(state=State.COMPLETE, *, label_id=41, category="", error="", lifecycle=None):
    return SimpleNamespace(
        label_id=label_id,
        state=state,
        lifecycle=lifecycle or state,
        category=category,
        error=error,
        park_reason="NO_WORKERS" if state == State.PARKED else "",
        park_attempts=2 if state == State.PARKED else 0,
        next_retry_at_ms=123 if state == State.PARKED else 0,
        worker_id=7,
        attempt=1,
    )


class FakeOperation:
    def __init__(self, waited=None, data=b"", cancellation=Cancellation.CANCELLED):
        self.waited = waited or SimpleNamespace(
            state=State.COMPLETE, results=[completion()])
        self.data = data
        self.cancellation = cancellation
        self.waits = []
        self.cancel_calls = 0

    def label_id(self):
        return self.waited.results[0].label_id

    def wait_all(self, timeout_ms):
        self.waits.append(timeout_ms)
        return self.waited

    def test(self):
        return self.waited.results[0]

    def cancel(self):
        self.cancel_calls += 1
        return [SimpleNamespace(
            state=self.cancellation,
            completion=self.waited.results[0],
        )]

    def read(self, timeout_ms):
        assert timeout_ms == 0
        return self.data


class FakeClient:
    def __init__(self, operations=()):
        self.operations = list(operations)
        self.labels = []
        self.published = []
        self.observe_calls = []
        self.inspected = None
        # In-memory clio:// blob store, keyed exactly like ClioCoreBackend's
        # real tag_and_blob() mapping (host=tag authority, stream=blob path)
        # -- lets ToolCache(ClioLabelBackend(...)) round-trip against this
        # fake without a real clio_run runtime.
        self.clio_store: dict[tuple[str, str], bytes] = {}

    def _clio_key(self, resource):
        if resource is None or getattr(resource, "backend_id", "") != "clio":
            return None
        return (resource.host, resource.stream)

    def create_label(self, params):
        label = SimpleNamespace(
            id=41 + len(self.labels),
            ir_version=1,
            operation="core.read" if params.type == labios.LabelType.Read else "core.write",
            operation_version=1,
            type=params.type,
            source_resource=params.source_resource,
            destination_resource=params.destination_resource,
            intent=params.intent,
            priority=params.priority,
            ttl_seconds=params.ttl_seconds,
            pipeline=params.pipeline,
            data_size=0,
            placement_history=SimpleNamespace(decisions=[]),
        )
        self.labels.append(label)
        return label

    def publish(self, label, data=b""):
        self.published.append((label, bytes(data)))
        if label.type == labios.LabelType.Write:
            key = self._clio_key(label.destination_resource)
            if key is not None:
                self.clio_store[key] = bytes(data)
                return FakeOperation()
        elif label.type == labios.LabelType.Read:
            key = self._clio_key(label.source_resource)
            if key is not None:
                stored = self.clio_store.get(key)
                if stored is None:
                    return FakeOperation(SimpleNamespace(
                        state=State.FAILED,
                        results=[completion(State.FAILED, category="EXECUTION_FAILED",
                                            error="EXECUTION_FAILED: blob not found")],
                    ))
                return FakeOperation(data=stored)
        return self.operations.pop(0) if self.operations else FakeOperation()

    def observe(self, query):
        self.observe_calls.append(query)
        return '{"scheduler_policy":"round-robin","scheduler_profile":"agentic"}'

    def inspect_label(self, label_id):
        self.inspected = label_id
        label = self.labels[-1]
        label.placement_history = SimpleNamespace(decisions=[SimpleNamespace(
            decision_id=1, attempt=1, outcome="Assigned", chosen_worker_id=7,
            park_reason="", policy_name="round-robin")])
        return label

    def operation(self, ids):
        assert ids == [99]
        return FakeOperation(SimpleNamespace(
            state=State.PENDING,
            results=[completion(State.PENDING, label_id=99)],
        ))


@pytest.mark.anyio
async def test_tool_schemas_and_imports_are_public_label_io():
    tools = {tool.name: tool for tool in await list_tools()}
    assert set(tools) == {
        "labios_observe", "labios_store", "labios_retrieve",
        "labios_process", "labios_knowledge",
    }
    assert tools["labios_store"].inputSchema["required"] == ["destination", "data"]
    assert "scope" not in tools["labios_store"].inputSchema["properties"]
    assert tools["labios_process"].inputSchema["required"] == [
        "source", "destination", "pipeline"]
    assert labios.Operation is not None


def test_store_lowers_typed_destination_and_preserves_intent_priority_ttl():
    client = FakeClient()
    frontend = McpFrontend(client)
    result = frontend.call("labios_store", {
        "destination": "file:///mcp/unit.bin",
        "data": base64.b64encode(b"\x00exact\xff").decode(),
        "encoding": "base64",
        "intent": "tool_output",
        "priority": 213,
        "ttl_seconds": 17,
        "timeout_ms": 4321,
    })

    label, staged = client.published[0]
    assert result["status"] == "completed"
    assert staged == b"\x00exact\xff"
    assert label.type == labios.LabelType.Write
    assert label.destination_resource.family == labios.ResourceFamily.FILE_RANGE
    assert label.destination_resource.path == "/mcp/unit.bin"
    assert label.intent == labios.Intent.TOOL_OUTPUT
    assert label.priority == 213
    assert label.ttl_seconds == 17
    assert client.operations == []


def test_retrieve_lowers_typed_source_and_returns_exact_public_bytes():
    operation = FakeOperation(data=b"\x00read\xff")
    client = FakeClient([operation])
    result = McpFrontend(client).call("labios_retrieve", {
        "source": "sqlite:///mcp/item",
        "size": 7,
        "encoding": "base64",
        "intent": "cache",
        "priority": 9,
    })

    label, staged = client.published[0]
    assert staged == b""
    assert label.type == labios.LabelType.Read
    assert label.source_resource.family == labios.ResourceFamily.RELATIONAL
    assert label.source_resource.path == "/mcp/item"
    assert label.data_size == 7
    assert label.intent == labios.Intent.CACHE
    assert label.priority == 9
    assert base64.b64decode(result["data"]) == b"\x00read\xff"


def test_process_preserves_source_destination_structured_pipeline_and_intent():
    client = FakeClient()
    result = McpFrontend(client).call("labios_process", {
        "source": "file:///mcp/source",
        "destination": "sqlite:///mcp/destination",
        "pipeline": [
            {"operation": "builtin://identity", "args": ""},
            {"operation": "builtin://truncate", "args": "4", "input_stage": 0},
        ],
        "intent": "intermediate",
        "priority": 177,
    })

    label, staged = client.published[0]
    assert result["status"] == "completed"
    assert staged == b""
    assert label.source_resource.path == "/mcp/source"
    assert label.destination_resource.path == "/mcp/destination"
    assert label.intent == labios.Intent.INTERMEDIATE
    assert label.priority == 177
    assert [(s.operation, s.args, s.input_stage) for s in label.pipeline.stages] == [
        ("builtin://identity", "", -1),
        ("builtin://truncate", "4", 0),
    ]


def test_stable_admission_and_execution_error_mapping():
    class RejectingClient(FakeClient):
        def create_label(self, params):
            raise labios.ResourceError("UNKNOWN_RESOURCE: not registered")

    admission = McpFrontend(RejectingClient()).call("labios_store", {
        "destination": "file:///ignored", "data": "x"})
    assert admission["status"] == "admission_failure"
    assert admission["error"]["category"] == "UNKNOWN_RESOURCE"

    failed = FakeOperation(SimpleNamespace(
        state=State.FAILED,
        results=[completion(State.FAILED, category="EXECUTION_FAILED",
                            error="EXECUTION_FAILED: backend write failed")],
    ))
    execution = McpFrontend(FakeClient([failed])).call("labios_store", {
        "destination": "file:///failure", "data": "x"})
    assert execution["status"] == "execution_failure"
    assert execution["error"]["category"] == "EXECUTION_FAILED"


def test_timeout_and_cancellation_are_distinct_and_operation_owned():
    timeout_wait = SimpleNamespace(
        state=State.TIMEOUT,
        results=[completion(State.PENDING, lifecycle=State.PENDING)],
    )
    active = FakeOperation(timeout_wait)
    timed_out = McpFrontend(FakeClient([active])).call("labios_store", {
        "destination": "file:///timeout", "data": "x", "timeout_ms": 0,
    })
    assert timed_out["status"] == "timeout"
    assert timed_out["error"]["retryable"] is True
    assert active.cancel_calls == 0

    cancelled = FakeOperation(timeout_wait, cancellation=Cancellation.CANCELLED)
    projected = McpFrontend(FakeClient([cancelled])).call("labios_store", {
        "destination": "file:///cancel", "data": "x", "timeout_ms": 0,
        "cancel_on_timeout": True,
    })
    assert projected["status"] == "cancelled"
    assert projected["error"]["category"] == "CANCELED"
    assert cancelled.cancel_calls == 1

    too_late = FakeOperation(timeout_wait, cancellation=Cancellation.TOO_LATE)
    raced = McpFrontend(FakeClient([too_late])).call("labios_store", {
        "destination": "file:///race", "data": "x", "timeout_ms": 0,
        "cancel_on_timeout": True,
    })
    assert raced["status"] == "cancellation"
    assert raced["error"]["category"] == "CANCELLATION_TOO_LATE"


def test_parked_and_unknown_completion_projection():
    parked_wait = SimpleNamespace(
        state=State.TIMEOUT,
        results=[completion(State.PARKED, lifecycle=State.PARKED)],
    )
    parked = McpFrontend(FakeClient([FakeOperation(parked_wait)])).call(
        "labios_store", {"destination": "file:///parked", "data": "x"})
    assert parked["status"] == "parked"
    assert parked["completion"]["park_reason"] == "NO_WORKERS"

    unknown_wait = SimpleNamespace(
        state=State.UNKNOWN, results=[completion(State.UNKNOWN)])
    unknown = McpFrontend(FakeClient([FakeOperation(unknown_wait)])).call(
        "labios_store", {"destination": "file:///unknown", "data": "x"})
    assert unknown["status"] == "completion_unknown"


@pytest.mark.parametrize("arguments", [
    {},
    {"destination": "file:///x", "data": "%%%", "encoding": "base64"},
    {"destination": "file:///x", "data": "x", "priority": 256},
    {"destination": "file:///x", "data": "x", "timeout_ms": "soon"},
    {"destination": "file:///x", "data": "x", "scope": "project/legacy"},
])
def test_malformed_store_requests_are_rejected_before_client_use(arguments):
    client = FakeClient()
    result = McpFrontend(client).call("labios_store", arguments)
    assert result["status"] == "malformed_request"
    assert result["error"]["category"] == "MALFORMED_REQUEST"
    assert not client.published


def test_unsupported_arbitrary_transformations_are_explicitly_rejected():
    client = FakeClient()
    for operation in ("grep:TODO", "shell://rm", "repo://arbitrary"):
        result = McpFrontend(client).call("labios_process", {
            "source": "file:///source",
            "destination": "file:///destination",
            "pipeline": [{"operation": operation}],
        })
        assert result["status"] == "malformed_request"
        assert "not a registered Label I/O transform" in result["error"]["message"]
    assert not client.published


def test_observe_uses_only_public_observe_and_inspection_apis():
    client = FakeClient()
    frontend = McpFrontend(client)
    active = frontend.call("labios_observe", {"query": "config/current"})
    assert active["observation"]["scheduler_policy"] == "round-robin"
    assert client.observe_calls == ["config/current"]

    frontend.call("labios_store", {"destination": "file:///inspect", "data": "x"})
    inspected = frontend.call("labios_observe", {
        "query": "label/inspect", "label_id": 41})
    assert inspected["label"]["ir_version"] == 1
    assert inspected["label"]["destination"]["family"] == "file_range"
    assert inspected["label"]["placement_history"][0]["chosen_worker_id"] == 7
    assert client.inspected == 41


def test_normal_core_paths_have_no_direct_store_transport_or_volume_adapter():
    import labios_mcp.server as server

    source = inspect.getsource(server)
    forbidden = (
        "import redis", "redis.asyncio", "import nats", "nats.connect",
        "labios.manager.workers", "/labios/data", "pathlib.Path",
    )
    assert all(token not in source for token in forbidden)

    client = FakeClient([FakeOperation(data=b"x") for _ in range(3)])
    frontend = McpFrontend(client)
    assert frontend.call("labios_store", {
        "destination": "file:///proof", "data": "x"})["ok"]
    assert frontend.call("labios_retrieve", {
        "source": "file:///proof", "size": 1})["ok"]
    assert frontend.call("labios_process", {
        "source": "file:///proof", "destination": "sqlite:///proof",
        "pipeline": [{"operation": "builtin://identity"}],
    })["ok"]
    assert len(client.published) == 3


def test_knowledge_is_explicitly_pending_prompt_14():
    result = McpFrontend(FakeClient()).call("labios_knowledge", {})
    assert result["status"] == "unsupported_feature"
    assert result["error"]["category"] == "WORKSPACE_KNOWLEDGE_UNAVAILABLE"


def _worker(builder, worker_id=7):
    FbWorker.WorkerDescriptorStart(builder)
    FbWorker.WorkerDescriptorAddId(builder, worker_id)
    FbWorker.WorkerDescriptorAddRegistrationEpoch(builder, 9)
    FbWorker.WorkerDescriptorAddAvailable(builder, True)
    FbWorker.WorkerDescriptorAddTotalCapacityBytes(builder, 4096)
    FbWorker.WorkerDescriptorAddAvailableCapacityBytes(builder, 2048)
    FbWorker.WorkerDescriptorAddCapacity(builder, 0.5)
    FbWorker.WorkerDescriptorAddLoad(builder, 0.25)
    FbWorker.WorkerDescriptorAddSpeed(builder, 4)
    FbWorker.WorkerDescriptorAddEnergy(builder, 2)
    FbWorker.WorkerDescriptorAddTier(builder, 1)
    FbWorker.WorkerDescriptorAddMaxIrVersion(builder, 1)
    return FbWorker.WorkerDescriptorEnd(builder)


def _snapshot_buffer(*, version=2):
    builder = flatbuffers.Builder(512)
    row = _worker(builder)
    FbSnapshot.WorkerRegistrySnapshotStartWorkersVector(builder, 1)
    builder.PrependUOffsetTRelative(row)
    workers = builder.EndVector()
    FbSnapshot.WorkerRegistrySnapshotStart(builder)
    FbSnapshot.WorkerRegistrySnapshotAddRegistryGeneration(builder, 1)
    FbSnapshot.WorkerRegistrySnapshotAddCapturedUs(builder, 123456)
    FbSnapshot.WorkerRegistrySnapshotAddWorkers(builder, workers)
    snapshot = FbSnapshot.WorkerRegistrySnapshotEnd(builder)
    FbMessage.WorkerRegistryMessageStart(builder)
    FbMessage.WorkerRegistryMessageAddProtocolVersion(builder, version)
    FbMessage.WorkerRegistryMessageAddKind(builder, PayloadKind.PayloadKind.Snapshot)
    FbMessage.WorkerRegistryMessageAddPayloadType(
        builder, Payload.Payload.WorkerRegistrySnapshot)
    FbMessage.WorkerRegistryMessageAddPayload(builder, snapshot)
    message = FbMessage.WorkerRegistryMessageEnd(builder)
    builder.Finish(message, file_identifier=b"LWR2")
    return bytes(builder.Output())


def test_mcp_registry_snapshot_uses_shared_verified_lwr2_parser_only():
    valid = _snapshot_buffer()
    assert [row.id for row in decode_registry_snapshot(valid).workers] == [7]
    with pytest.raises(MalformedRegistryBuffer):
        decode_registry_snapshot(valid[:9])
    with pytest.raises(MalformedRegistryBuffer):
        decode_registry_snapshot(b"7,1,0.5,0.2,4,2\n")
    with pytest.raises(UnsupportedRegistryVersion):
        decode_registry_snapshot(_snapshot_buffer(version=3))


def test_default_backend_respects_disable_env_var(monkeypatch):
    import labios_mcp.server as server

    monkeypatch.setenv("LABIOS_MCP_TOOL_CACHE", "0")
    assert isinstance(server._default_backend(client=object()), NullBackend)

    monkeypatch.setenv("LABIOS_MCP_TOOL_CACHE", "1")
    assert isinstance(server._default_backend(client=object()), ClioLabelBackend)


def test_default_frontend_cache_is_inert_and_never_touches_client():
    client = FakeClient([FakeOperation(data=b"y")])
    frontend = McpFrontend(client)
    frontend.call("labios_retrieve", {"source": "file:///z.bin", "size": 0})
    assert client.clio_store == {}


def test_retrieve_full_read_hits_shared_raw_source_cache_and_invalidates_on_store():
    client = FakeClient([FakeOperation(data=b"hello world")])
    cache = ToolCache(ClioLabelBackend(client), session_id="t1")
    frontend = McpFrontend(client, cache=cache)

    first = frontend.call("labios_retrieve", {"source": "file:///a.txt", "size": 0})
    cache.backend.wait_for_pending_puts()
    assert first["status"] == "completed"
    assert base64.b64decode(first["data"]) == b"hello world"
    assert not first.get("cached")

    # No more real operations queued -- a second identical retrieve must be
    # served from the raw-source cache, not a real fetch.
    second = frontend.call("labios_retrieve", {"source": "file:///a.txt", "size": 0})
    assert second["cached"] is True
    assert base64.b64decode(second["data"]) == b"hello world"

    # A store to the same URI invalidates the cached entry.
    frontend.call("labios_store", {"destination": "file:///a.txt", "data": "new"})
    client.operations.append(FakeOperation(data=b"new bytes on disk"))
    third = frontend.call("labios_retrieve", {"source": "file:///a.txt", "size": 0})
    assert not third.get("cached")
    assert base64.b64decode(third["data"]) == b"new bytes on disk"


def test_process_single_stage_safe_op_reuses_cached_raw_source_without_reexecuting_pipeline():
    client = FakeClient([FakeOperation(data=b"aabbccdd")])
    cache = ToolCache(ClioLabelBackend(client), session_id="t2")
    frontend = McpFrontend(client, cache=cache)

    frontend.call("labios_retrieve", {"source": "file:///src.bin", "size": 0})
    cache.backend.wait_for_pending_puts()

    published_before = len(client.published)
    result = frontend.call("labios_process", {
        "source": "file:///src.bin",
        "destination": "sqlite:///dst",
        "pipeline": [{"operation": "builtin://truncate", "args": "4"}],
    })
    assert result["status"] == "completed"
    # Three new publishes: the exact-key cache-miss check, the raw-source
    # cache-hit check, and the real destination write -- no source re-fetch
    # and no pipeline-carrying label sent to a worker; truncate ran locally
    # against the already-cached raw source bytes. Even a "fast path" hit
    # still pays for cache lookups as real label round trips (see
    # dazzling-leaping-sprout.md's "Decision" section) -- this only skips
    # the source re-fetch and worker-side pipeline execution.
    assert len(client.published) == published_before + 3
    label, staged = client.published[-1]
    assert label.destination_resource.path == "/dst"
    assert staged == b"aabb"


def test_process_exact_repeat_is_served_from_cache():
    client = FakeClient([FakeOperation(data=b"x")])
    cache = ToolCache(ClioLabelBackend(client), session_id="t3")
    frontend = McpFrontend(client, cache=cache)

    call_args = {
        "source": "file:///src2.bin",
        "destination": "sqlite:///dst2",
        "pipeline": [{"operation": "builtin://identity"}],
    }
    first = frontend.call("labios_process", call_args)
    cache.backend.wait_for_pending_puts()
    assert first["status"] == "completed"
    assert not first.get("cached")

    published_before = len(client.published)
    second = frontend.call("labios_process", call_args)
    assert second.get("cached") is True
    # A hit still costs exactly one label round trip (the cache lookup
    # itself) -- it replaces re-executing the pipeline, it doesn't make the
    # call free. See dazzling-leaping-sprout.md's "Decision" section.
    assert len(client.published) == published_before + 1


def test_tool_cache_stats_observable_via_mcp_observe():
    client = FakeClient([FakeOperation(data=b"content")])
    cache = ToolCache(ClioLabelBackend(client), session_id="t4")
    frontend = McpFrontend(client, cache=cache)

    frontend.call("labios_retrieve", {"source": "file:///stats.bin", "size": 0})
    cache.backend.wait_for_pending_puts()
    frontend.call("labios_retrieve", {"source": "file:///stats.bin", "size": 0})

    stats = frontend.call("labios_observe", {"query": "mcp/tool_cache_stats"})
    assert stats["ok"] is True
    assert stats["observation"]["hits"] >= 1
    assert stats["observation"]["stores"] >= 1
