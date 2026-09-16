#!/usr/bin/env bash
# Sourceable library (NOT an sbatch job itself) that starts a full Labios
# stack -- clio_run, NATS, Redis, labios-manager, labios-dispatcher, one
# labios-worker with backends.clio_enabled=true -- as plain processes on the
# current compute node, no Docker required. Written for the SWE-bench/Labios
# tool-cache comparison (SWE-bench/cache_perf_matrix_labios.sh sources this),
# but it stands up the same topology docker-compose.yml describes, so it's
# reusable for any other live test of the clio:// backend end-to-end
# (including mcp/tests/test_live.py's LABIOS_MCP_LIVE=1 cases).
#
# Must run on a compute node, not the login node: clio_run + the CTE client
# share IPC/shared-memory state (see sbatch_clio_backend.sh's header for the
# same constraint) -- this is true for start_labios_stack too, since it also
# launches clio_run.
#
# Prerequisites (build once, ahead of time -- this script does not build):
#   cmake -S /work/hdd/bekn/kbateman/labios -B /work/hdd/bekn/kbateman/labios/build-clio \
#         -DLABIOS_ENABLE_CLIO_BACKEND=ON
#   cmake --build /work/hdd/bekn/kbateman/labios/build-clio \
#         --target labios-manager labios-dispatcher labios-worker
#   (the plain, non-clio build's build/python/_labios*.so is used for the
#   Python/MCP side -- LABIOS_ENABLE_CLIO_BACKEND only gates the worker's C++
#   backend adapter, not the Python-visible label/resource API, so it does
#   NOT need to be built with that flag; see label.cpp's parse_resource.)
# Also needed once, not built here (see this repo's own scripts):
#   install/bin/nats-server        (static release binary, any recent 2.10.x)
#   install/bin/clio_run           (built from clio-core)
#   envs/labios-live/bin/redis-server   (e.g. `conda create -p envs/labios-live -c conda-forge redis-server`)
#   envs/labios-mcp-venv/bin/python3    (venv with `pip install -r labios/mcp/requirements.txt`)
#
# Usage (source it, don't execute it):
#   source /work/hdd/bekn/kbateman/labios/tests/live/start_labios_stack.sh
#   start_labios_stack "$JOB_STATE_DIR"        # exports LABIOS_NATS_URL etc.
#   ...run whatever needs the stack...
#   stop_labios_stack                          # or rely on the caller's own EXIT trap
#
# Ports are derived from $SLURM_JOB_ID (falls back to $$ outside Slurm) so
# two jobs co-scheduled on the same node don't collide -- see
# cache_perf_matrix.sh's own bug-fix comment for exactly this failure mode
# with a hardcoded clio port.

LABIOS_STACK_ROOT="${LABIOS_STACK_ROOT:-/work/hdd/bekn/kbateman/labios}"
LABIOS_STACK_INSTALL="${LABIOS_STACK_INSTALL:-/work/hdd/bekn/kbateman/install}"
LABIOS_STACK_BUILD_CLIO="${LABIOS_STACK_BUILD_CLIO:-$LABIOS_STACK_ROOT/build-clio}"
LABIOS_STACK_REDIS_ENV="${LABIOS_STACK_REDIS_ENV:-/work/hdd/bekn/kbateman/envs/labios-live}"

_labios_stack_pids=()

start_labios_stack() {
    local job_state_dir="${1:?usage: start_labios_stack <job_state_dir>}"
    mkdir -p "$job_state_dir"
    local job_id="${SLURM_JOB_ID:-$$}"

    # Job-scoped ports, one contiguous block per job so collisions across
    # co-scheduled jobs on the same node are as unlikely as OLLAMA_PORT's own
    # scheme in cache_perf_matrix.sh.
    LABIOS_STACK_NATS_PORT=$(( 41000 + job_id % 4000 ))
    LABIOS_STACK_NATS_MON_PORT=$(( LABIOS_STACK_NATS_PORT + 1 ))
    LABIOS_STACK_REDIS_PORT=$(( 46000 + job_id % 4000 ))
    LABIOS_STACK_CLIO_PORT=$(( 51000 + job_id % 4000 ))

    echo "[labios-stack] job_state_dir=$job_state_dir nats=$LABIOS_STACK_NATS_PORT redis=$LABIOS_STACK_REDIS_PORT clio=$LABIOS_STACK_CLIO_PORT"

    # ── clio_run ─────────────────────────────────────────────────────────
    # Same minimal RAM-backed compose as sbatch_clio_backend.sh -- a bare CTE
    # core pool, explicit small segment sizes so this never tries to size
    # itself off total node RAM.
    local clio_conf="$job_state_dir/clio_compose.yaml"
    cat > "$clio_conf" <<EOF
networking:
  port: ${LABIOS_STACK_CLIO_PORT}
runtime:
  num_threads: 2
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
      metadata_log_path: "$job_state_dir/cte_metadata_log"
      transaction_log_capacity: "8MB"
    dpe:
      dpe_type: "max_bw"
    targets:
      neighborhood: 1
EOF
    export CLIO_SERVER_CONF="$clio_conf"
    export LD_LIBRARY_PATH="$LABIOS_STACK_INSTALL/lib:$LABIOS_STACK_INSTALL/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    "$LABIOS_STACK_INSTALL/bin/clio_run" start > "$job_state_dir/clio_run.log" 2>&1 &
    _labios_stack_pids+=($!)
    sleep 10  # generous warmup -- see sbatch_clio_backend.sh's comment on why a single sleep beats polling status here

    # ── NATS (JetStream) ─────────────────────────────────────────────────
    mkdir -p "$job_state_dir/nats-data"
    "$LABIOS_STACK_INSTALL/bin/nats-server" -p "$LABIOS_STACK_NATS_PORT" -m "$LABIOS_STACK_NATS_MON_PORT" \
        --jetstream --store_dir="$job_state_dir/nats-data" > "$job_state_dir/nats.log" 2>&1 &
    _labios_stack_pids+=($!)

    # ── Redis (main data-plane store; ContentManager/CatalogManager) ─────
    # --dir pins the RDB snapshot to the job's own scratch dir -- without it,
    # redis-server dumps dump.rdb into whatever the caller's CWD happened to
    # be (e.g. the labios repo root, if sbatch was submitted from there).
    mkdir -p "$job_state_dir/redis-data"
    "$LABIOS_STACK_REDIS_ENV/bin/redis-server" --port "$LABIOS_STACK_REDIS_PORT" --daemonize no \
        --dir "$job_state_dir/redis-data" \
        > "$job_state_dir/redis.log" 2>&1 &
    _labios_stack_pids+=($!)

    for i in $(seq 1 30); do
        curl -sf "http://127.0.0.1:${LABIOS_STACK_NATS_MON_PORT}/healthz" >/dev/null 2>&1 \
            && "$LABIOS_STACK_REDIS_ENV/bin/redis-cli" -p "$LABIOS_STACK_REDIS_PORT" ping >/dev/null 2>&1 \
            && break
        sleep 1
    done
    if ! curl -sf "http://127.0.0.1:${LABIOS_STACK_NATS_MON_PORT}/healthz" >/dev/null 2>&1; then
        echo "[labios-stack] FAILED: nats-server did not become healthy -- see $job_state_dir/nats.log" >&2
        return 1
    fi
    if ! "$LABIOS_STACK_REDIS_ENV/bin/redis-cli" -p "$LABIOS_STACK_REDIS_PORT" ping >/dev/null 2>&1; then
        echo "[labios-stack] FAILED: redis-server did not become healthy -- see $job_state_dir/redis.log" >&2
        return 1
    fi
    echo "[labios-stack] nats + redis healthy"

    export LABIOS_NATS_URL="nats://127.0.0.1:${LABIOS_STACK_NATS_PORT}"
    export LABIOS_REDIS_HOST="127.0.0.1"
    export LABIOS_REDIS_PORT="${LABIOS_STACK_REDIS_PORT}"
    export LABIOS_STORAGE_ROOT="$job_state_dir/storage"
    mkdir -p "$LABIOS_STORAGE_ROOT"

    # ── labios-manager / -dispatcher / -worker (clio_enabled=true) ───────
    local labios_conf="$job_state_dir/labios.toml"
    cat > "$labios_conf" <<'EOF'
[nats]
max_deliver = 5
ack_wait_ms = 10000
[worker]
id = 1
speed = 1
capacity = "1GB"
[client]
reply_timeout_ms = 30000
[label]
min_size = "64KB"
max_size = "1MB"
[cache]
flush_interval_ms = 500
default_read_policy = "read-through"
[dispatcher]
batch_size = 100
batch_timeout_ms = 50
aggregation_enabled = true
dep_granularity = "per-file"
[scheduler]
policy = "round-robin"
[backends]
file_enabled = true
sqlite_enabled = true
kv_enabled = false
clio_enabled = true
clio_tag_prefix = "labios:"
[elastic]
enabled = false
EOF
    export LABIOS_CONFIG_PATH="$labios_conf"

    "$LABIOS_STACK_BUILD_CLIO/src/services/labios-manager" > "$job_state_dir/manager.log" 2>&1 &
    _labios_stack_pids+=($!)
    sleep 3
    "$LABIOS_STACK_BUILD_CLIO/src/services/labios-dispatcher" > "$job_state_dir/dispatcher.log" 2>&1 &
    _labios_stack_pids+=($!)
    sleep 3
    LABIOS_WORKER_ID=1 LABIOS_WORKER_TIER=1 \
        "$LABIOS_STACK_BUILD_CLIO/src/services/labios-worker" > "$job_state_dir/worker.log" 2>&1 &
    _labios_stack_pids+=($!)
    sleep 4

    if ! grep -q "manager ready" "$job_state_dir/manager.log" 2>/dev/null \
        || ! grep -q "dispatcher ready" "$job_state_dir/dispatcher.log" 2>/dev/null \
        || ! grep -q "worker-1 ready" "$job_state_dir/worker.log" 2>/dev/null; then
        echo "[labios-stack] FAILED: manager/dispatcher/worker did not all report ready -- see $job_state_dir/{manager,dispatcher,worker}.log" >&2
        return 1
    fi
    echo "[labios-stack] manager + dispatcher + worker ready (clio_enabled=true)"
}

stop_labios_stack() {
    echo "[labios-stack] stopping: ${_labios_stack_pids[*]:-none}"
    for p in "${_labios_stack_pids[@]:-}"; do kill "$p" 2>/dev/null; done
    sleep 2
    for p in "${_labios_stack_pids[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    "$LABIOS_STACK_INSTALL/bin/clio_run" stop --force > /dev/null 2>&1 || true
    _labios_stack_pids=()
}
