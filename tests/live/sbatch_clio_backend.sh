#!/usr/bin/env bash
# Live smoke test for ClioCoreBackend (clio:// scheme): starts a real clio_run
# runtime and exercises the [clio][live] Catch2 cases in labios-backend-test
# against it (see include/labios/backend/clio_backend.h, docs/backends.md).
#
# Must run on a compute node, not the login node: the CTE client and the
# clio_run daemon share IPC/shared-memory state for the actual blob data path
# (CLIO_IPC->AllocateBuffer), which isn't visible across nodes, and starting a
# persistent service on a login node is inappropriate anyway.
#
# Prerequisite: build labios with the clio backend enabled first --
#   cmake -S /work/hdd/bekn/kbateman/labios -B /work/hdd/bekn/kbateman/labios/build-clio \
#         -DLABIOS_ENABLE_CLIO_BACKEND=ON
#   cmake --build /work/hdd/bekn/kbateman/labios/build-clio --target labios-backend-test
#
# NOT submitted automatically -- review and `sbatch` this yourself:
#   sbatch /work/hdd/bekn/kbateman/labios/tests/live/sbatch_clio_backend.sh
#
#SBATCH --job-name=labios-clio-live
#SBATCH --partition=cpu
#SBATCH --account=bekn-delta-cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=00:20:00
#SBATCH --output=/work/hdd/bekn/kbateman/labios/tests/live/logs/clio-live-%j.out
#SBATCH --error=/work/hdd/bekn/kbateman/labios/tests/live/logs/clio-live-%j.err

set -uo pipefail

LABIOS=/work/hdd/bekn/kbateman/labios
INSTALL=/work/hdd/bekn/kbateman/install
CLIO_RUN="$INSTALL/bin/clio_run"
BACKEND_TEST="$LABIOS/build-clio/tests/labios-backend-test"
LOG_DIR="$LABIOS/tests/live/logs"
JOB_STATE_DIR="/tmp/labios_clio_smoke_${SLURM_JOB_ID:-manual}"

mkdir -p "$LOG_DIR" "$JOB_STATE_DIR"

# Mirror ~/.bashrc's LOCAL_INSTALL_PREFIX block explicitly rather than relying
# on an interactive shell's rc file in this non-interactive sbatch shell.
export CMAKE_PREFIX_PATH="$INSTALL${CONDA_PREFIX:+:$CONDA_PREFIX}${CMAKE_PREFIX_PATH:+:$CMAKE_PREFIX_PATH}"
export PATH="$INSTALL/bin:$PATH"
export LIBRARY_PATH="$INSTALL/lib:$INSTALL/lib64${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$INSTALL/lib:$INSTALL/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

if [[ ! -x "$BACKEND_TEST" ]]; then
    echo "labios-backend-test not built at $BACKEND_TEST" >&2
    echo "Build it first (see this script's header comment)." >&2
    exit 1
fi

# Minimal compose: RAM block device + a bare CTE core pool (no replication /
# cache / indexer / filesystem chain -- the backend talks to pool 512.0
# directly and none of that is needed for a put/get/del/query smoke test).
# Capacities are pinned small and explicit rather than relying on the
# clio_default.yaml convention of "0g = 80% of system DRAM", which could
# wildly overshoot this job's --mem reservation on a large-memory node.
COMPOSE_YAML="$JOB_STATE_DIR/compose.yaml"
cat > "$COMPOSE_YAML" <<EOF
networking:
  port: 9413
runtime:
  num_threads: 2
  # Explicit and small: CalculateMainSegmentSize()/CalculateMetadataSegmentSize()
  # otherwise auto-size these from total node RAM (not this job's --mem
  # cgroup), which on a large-memory node means requesting on the order of
  # 100GB+ and never finishing init within the readiness wait below.
  main_segment_size: "2GB"
  metadata_segment_size: "256MB"
compose:
  - mod_name: clio_bdev
    pool_name: "ram::chi_default_bdev"
    pool_query: local
    pool_id: "301.0"
    bdev_type: ram
    capacity: "512MB"

  - mod_name: clio_cte_core
    pool_name: cte_main
    pool_query: local
    pool_id: "512.0"
    storage:
      - path: "ram::cte_ram_tier1"
        bdev_type: "ram"
        capacity_limit: "256MB"
        score: 1.0
    performance:
      metadata_log_path: "$JOB_STATE_DIR/cte_metadata_log"
      transaction_log_capacity: "8MB"
    dpe:
      dpe_type: "max_bw"
    targets:
      neighborhood: 1
EOF

export CLIO_SERVER_CONF="$COMPOSE_YAML"

cleanup() {
    echo "[cleanup] stopping clio_run"
    "$CLIO_RUN" stop --force > "$LOG_DIR/clio_run-stop-${SLURM_JOB_ID:-manual}.log" 2>&1 || true
    rm -rf "$JOB_STATE_DIR"
}
trap cleanup EXIT

echo "[clio_run] starting on $(hostname), config=$COMPOSE_YAML"
"$CLIO_RUN" start > "$LOG_DIR/clio_run-${SLURM_JOB_ID:-manual}.log" 2>&1 &

# A single generous warmup sleep, then ONE status check for diagnostics only
# (not a gate -- see below for why). Each `clio_run status` invocation is a
# short-lived client process that registers its own ~570MB shm allocator
# against main_segment_size and doesn't get cleaned up until swept as a
# "stale artifact" on the next stop/status call; polling in a tight loop
# exhausts a 1GB main segment after a couple of iterations and produces
# "RouteTask: RouteLocal returned 4" admin-routing errors on later polls,
# so status ends up reporting UNRESPONSIVE rather than RUNNING even though
# the runtime is fine. A single status probe after warmup does not hit this.
sleep 10
echo "[clio_run] status after warmup:"
"$CLIO_RUN" status 2>&1 || true

echo "[test] running [clio] backend tests"
"$BACKEND_TEST" "[clio]"
TEST_EXIT=$?

echo "[test] exit code: $TEST_EXIT"
exit "$TEST_EXIT"
