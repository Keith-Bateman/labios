#!/usr/bin/env bash
# Live, compute-node verification that clio:// actually works end-to-end now
# that both prerequisite bugs are fixed:
#   1. parse_resource() had no "clio" scheme case (admission-time rejection).
#   2. derive_worker_capabilities()/family_scheme() never advertised/matched
#      a "clio" attachment (scheduling-time: label parks forever).
# Both are covered by unit tests (tests/unit/label_test.cpp "[clio]",
# tests/unit/scheduling_feasibility_test.cpp "[clio]") but neither exercises
# a REAL clio_run + full worker pipeline. This job does:
#   1. labios-backend-test "[clio]" -- direct ClioCoreBackend put/get/del
#      roundtrip (no scheduler involved), the existing sbatch_clio_backend.sh
#      coverage, run here too as a sanity floor.
#   2. A real MCP round trip (labios_store + labios_retrieve over clio://)
#      through the actual admission -> scheduling -> worker -> backend path.
#   3. SWE-bench's LabiosMcpBackend get/put/get, confirming a REAL cache hit
#      (not just graceful degradation) now that clio_enabled=true.
#
# Submit:
#   sbatch /work/hdd/bekn/kbateman/labios/tests/live/sbatch_verify_clio_scheduling.sh
#
#SBATCH --job-name=labios-clio-verify
#SBATCH --partition=cpu
#SBATCH --account=bekn-delta-cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=00:20:00
#SBATCH --output=/work/hdd/bekn/kbateman/labios/tests/live/logs/clio-verify-%j.out
#SBATCH --error=/work/hdd/bekn/kbateman/labios/tests/live/logs/clio-verify-%j.err

set -uo pipefail

LABIOS=/work/hdd/bekn/kbateman/labios
SWEBENCH=/work/hdd/bekn/kbateman/SWE-bench
MCP_VENV=/work/hdd/bekn/kbateman/envs/labios-mcp-venv
JOB_STATE_DIR="/tmp/labios_clio_verify_${SLURM_JOB_ID:-manual}"
mkdir -p "$LABIOS/tests/live/logs"

overall_pass=1

source "$LABIOS/tests/live/start_labios_stack.sh"

cleanup() {
  stop_labios_stack
  rm -rf "$JOB_STATE_DIR"
}
trap cleanup EXIT

echo "=== [1/4] starting full labios stack on $(hostname) (clio_enabled=true) ==="
if ! start_labios_stack "$JOB_STATE_DIR"; then
  echo "[FAIL] stack did not start -- see $JOB_STATE_DIR/*.log"
  exit 1
fi

echo ""
echo "=== [2/4] labios-backend-test [clio] (direct ClioCoreBackend roundtrip, no scheduler) ==="
if "$LABIOS/build-clio/tests/labios-backend-test" "[clio]"; then
  echo "[PASS] backend-test [clio]"
else
  echo "[FAIL] backend-test [clio]"
  overall_pass=0
fi

echo ""
echo "=== [3/4] real MCP roundtrip through admission -> scheduling -> worker -> ClioCoreBackend ==="
PYTHONPATH="$LABIOS/mcp:$LABIOS/build/python" "$MCP_VENV/bin/python3" - <<'PYEOF'
import asyncio, base64, json, os, sys, time
from contextlib import AsyncExitStack
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "labios_mcp"], env=dict(os.environ))
    async with AsyncExitStack() as stack:
        read, write = await stack.enter_async_context(stdio_client(params))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()

        uri = "clio://verify-job/roundtrip.bin"
        payload = b"live clio verification payload"

        t0 = time.time()
        stored = await session.call_tool("labios_store", {
            "destination": uri, "data": base64.b64encode(payload).decode(),
            "encoding": "base64", "timeout_ms": 8000,
        })
        print(f"store took {time.time() - t0:.2f}s:", stored.content[0].text if stored.content else stored)

        t0 = time.time()
        fetched = await session.call_tool("labios_retrieve", {
            "source": uri, "encoding": "base64", "timeout_ms": 8000,
        })
        print(f"retrieve took {time.time() - t0:.2f}s:", fetched.content[0].text if fetched.content else fetched)

        store_payload = json.loads(stored.content[0].text) if stored.content else {}
        fetch_payload = json.loads(fetched.content[0].text) if fetched.content else {}
        ok = (store_payload.get("ok") and fetch_payload.get("ok")
              and base64.b64decode(fetch_payload.get("data", "")) == payload)
        print("ROUNDTRIP:", "PASS" if ok else "FAIL")
        return 0 if ok else 1

sys.exit(asyncio.run(main()))
PYEOF
if [[ $? -eq 0 ]]; then
  echo "[PASS] MCP clio:// roundtrip"
else
  echo "[FAIL] MCP clio:// roundtrip"
  overall_pass=0
fi

echo ""
echo "=== [4/4] SWE-bench LabiosMcpBackend: real cache miss then real cache hit ==="
cd "$SWEBENCH/agent"
SWE_BENCH_TOOL_CACHE_INIT_TIMEOUT=60 "$SWEBENCH/.venv/bin/python3" -u - <<'PYEOF2'
import asyncio, sys, time
sys.path.insert(0, ".")
import tool_cache

# Verbose standalone connect first (bypasses LabiosMcpBackend's own
# background-thread wrapper) so a hang shows exactly which await is stuck,
# rather than just an opaque outer TimeoutError.
async def probe():
    from contextlib import AsyncExitStack
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    import os
    env = dict(os.environ)
    env["PYTHONPATH"] = "/work/hdd/bekn/kbateman/labios/mcp:/work/hdd/bekn/kbateman/labios/build/python"
    params = StdioServerParameters(
        command="/work/hdd/bekn/kbateman/envs/labios-mcp-venv/bin/python3",
        args=["-m", "labios_mcp"], env=env)
    t0 = time.time()
    async with AsyncExitStack() as stack:
        print(f"[{time.time()-t0:.2f}] entering stdio_client", flush=True)
        read, write = await stack.enter_async_context(stdio_client(params))
        print(f"[{time.time()-t0:.2f}] got streams, creating session", flush=True)
        session = await stack.enter_async_context(ClientSession(read, write))
        print(f"[{time.time()-t0:.2f}] initializing", flush=True)
        await asyncio.wait_for(session.initialize(), timeout=30)
        print(f"[{time.time()-t0:.2f}] initialized OK", flush=True)

try:
    asyncio.run(probe())
    print("standalone probe: PASS", flush=True)
except Exception as e:
    print(f"standalone probe: FAIL ({e!r})", flush=True)

print("--- now via the real LabiosMcpBackend class ---", flush=True)
t0 = time.time()
b = tool_cache.LabiosMcpBackend()
print(f"connected in {time.time()-t0:.2f}s", flush=True)
tag, blob = "swebench-clio-verify", "v1/hit-test"
miss = b.get(tag, blob)
print("initial get (expect None, nothing stored yet):", miss, flush=True)
b.put(tag, blob, b"cached from swebench, real clio hit")
time.sleep(2)
t0 = time.time()
hit = b.get(tag, blob)
print(f"get after put: {hit!r} in {time.time() - t0:.2f}s", flush=True)
b.close()

ok = miss is None and hit == b"cached from swebench, real clio hit"
print("SWE-BENCH BACKEND TEST:", "PASS" if ok else "FAIL", flush=True)
sys.exit(0 if ok else 1)
PYEOF2
if [[ $? -eq 0 ]]; then
  echo "[PASS] SWE-bench LabiosMcpBackend real cache hit"
else
  echo "[FAIL] SWE-bench LabiosMcpBackend real cache hit"
  overall_pass=0
fi

echo ""
if [[ $overall_pass -eq 1 ]]; then
  echo "[RESULT] ALL CHECKS PASSED"
else
  echo "[RESULT] SOME CHECKS FAILED -- see above"
fi
exit $((1 - overall_pass))
