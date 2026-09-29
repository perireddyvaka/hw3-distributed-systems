#!/usr/bin/env bash
# =============================================================================
#  hw3_grpc/scripts/run_rce_cluster.sh — Distributed RCE Cluster Execution
# =============================================================================
#  Runs the HW3 gRPC system across multiple compute nodes on the RCE cluster,
#  following the topology and guidelines in rce_grpc_execution_guide.pdf.
#
#  Usage:
#    # Automatically uses nodes from current salloc / Slurm job:
#    bash hw3_grpc/scripts/run_rce_cluster.sh
#
#    # Or specify nodes manually:
#    bash hw3_grpc/scripts/run_rce_cluster.sh --coord-node node01 --worker-nodes node02,node03
#
#    # Automated end-to-end multi-node demo:
#    bash hw3_grpc/scripts/run_rce_cluster.sh --demo
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# Source environment if available
if [ -f "$REPO_ROOT/cluster_env.sh" ]; then
    # shellcheck disable=SC1091
    source "$REPO_ROOT/cluster_env.sh"
elif [ -f "$REPO_ROOT/.venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$REPO_ROOT/.venv/bin/activate"
fi

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[rce_cluster]${RESET} $*"; }
ok()      { echo -e "${GREEN}[  OK  ]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[ WARN ]${RESET} $*"; }
fail()    { echo -e "${RED}[ FAIL ]${RESET} $*"; }
section() { echo -e "\n${BOLD}${YELLOW}══ $* ══${RESET}"; }

COORD_NODE=""
WORKER_NODES=()
COORD_PORT="${HW3_COORDINATOR_PORT:-50050}"
WORKER_PORT="${HW3_WORKER_BASE_PORT:-50060}"
DEMO_MODE=0
KEEP_ALIVE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --coord-node)    COORD_NODE="$2"; shift 2 ;;
        --worker-nodes)
            IFS=',' read -ra WORKER_NODES <<< "$2"
            shift 2 ;;
        --coord-port)    COORD_PORT="$2"; shift 2 ;;
        --worker-port)   WORKER_PORT="$2"; shift 2 ;;
        --demo)          DEMO_MODE=1; shift ;;
        --keep-alive)    KEEP_ALIVE=1; shift ;;
        --help|-h)
            echo "Usage: bash hw3_grpc/scripts/run_rce_cluster.sh [--demo] [--coord-node NODE] [--worker-nodes N1,N2]"
            exit 0
            ;;
        *)
            warn "Unknown argument: $1"; shift ;;
    esac
done

# Node detection if not manually supplied
if [ -z "$COORD_NODE" ]; then
    if [ -n "${SLURM_JOB_NODELIST:-}" ] && command -v scontrol >/dev/null 2>&1; then
        mapfile -t ALLOC_NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
        if [ "${#ALLOC_NODES[@]}" -ge 2 ]; then
            COORD_NODE="${ALLOC_NODES[0]}"
            WORKER_NODES=("${ALLOC_NODES[@]:1}")
            info "Auto-detected Slurm allocation nodes:"
            info "  Coordinator Node : $COORD_NODE"
            info "  Worker Nodes     : ${WORKER_NODES[*]}"
        elif [ "${#ALLOC_NODES[@]}" -eq 1 ]; then
            COORD_NODE="${ALLOC_NODES[0]}"
            WORKER_NODES=("${ALLOC_NODES[0]}")
            warn "Only 1 node allocated in Slurm ($COORD_NODE). Running multi-worker on this node."
        fi
    else
        # Fallback to localhost if no Slurm nodes detected
        COORD_NODE="localhost"
        WORKER_NODES=("localhost")
        warn "No Slurm allocation detected. Defaulting to localhost."
    fi
fi

PYTHON_CMD="python3"
if [ -f "$REPO_ROOT/.venv/bin/python3" ]; then
    PYTHON_CMD="$REPO_ROOT/.venv/bin/python3"
fi

PID_FILE="/tmp/hw3_rce_pids.txt"
rm -f "$PID_FILE"

cleanup() {
    echo ""
    info "Shutting down remote cluster processes..."
    if [ -f "$PID_FILE" ]; then
        while read -r host pid; do
            if [ "$host" = "localhost" ] || [ "$host" = "$(hostname)" ]; then
                kill "$pid" 2>/dev/null || true
            else
                ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$host" "kill $pid 2>/dev/null || true" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    info "Cluster stopped."
}
trap cleanup EXIT INT TERM

# ── Start Workers ─────────────────────────────────────────────────────────────
info "Launching ${#WORKER_NODES[@]} worker(s)..."
WORKER_ADDRS=()
worker_id=0

for w_node in "${WORKER_NODES[@]}"; do
    w_port=$((WORKER_PORT + worker_id))
    WORKER_ADDRS+=("${w_node}:${w_port}")
    info "  Starting Worker $worker_id on ${w_node}:${w_port} ..."

    REMOTE_SCRIPT="cd \"$REPO_ROOT\" && export PYTHONPATH=\"$REPO_ROOT:\$PYTHONPATH\" && nohup $PYTHON_CMD -m hw3_grpc.worker.worker_server --id $worker_id --port $w_port >> /tmp/hw3_worker_${worker_id}.log 2>&1 & echo \$!"

    if [ "$w_node" = "localhost" ] || [ "$w_node" = "$(hostname)" ]; then
        export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"
        nohup "$PYTHON_CMD" -m hw3_grpc.worker.worker_server --id "$worker_id" --port "$w_port" >> "/tmp/hw3_worker_${worker_id}.log" 2>&1 &
        W_PID=$!
    else
        W_PID=$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$w_node" "$REMOTE_SCRIPT")
    fi

    echo "$w_node $W_PID" >> "$PID_FILE"
    worker_id=$((worker_id + 1))
done

sleep 1

# ── Start Coordinator ─────────────────────────────────────────────────────────
WORKER_ADDR_STR=$(IFS=','; echo "${WORKER_ADDRS[*]}")
info "Starting Coordinator on ${COORD_NODE}:${COORD_PORT} connected to workers [${WORKER_ADDR_STR}] ..."

COORD_SCRIPT="cd \"$REPO_ROOT\" && export PYTHONPATH=\"$REPO_ROOT:\$PYTHONPATH\" && nohup $PYTHON_CMD -m hw3_grpc.coordinator.server --port $COORD_PORT --worker-addresses \"$WORKER_ADDR_STR\" >> /tmp/hw3_coordinator.log 2>&1 & echo \$!"

if [ "$COORD_NODE" = "localhost" ] || [ "$COORD_NODE" = "$(hostname)" ]; then
    export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"
    nohup "$PYTHON_CMD" -m hw3_grpc.coordinator.server --port "$COORD_PORT" --worker-addresses "$WORKER_ADDR_STR" >> "/tmp/hw3_coordinator.log" 2>&1 &
    C_PID=$!
else
    C_PID=$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "$COORD_NODE" "$COORD_SCRIPT")
fi

echo "$COORD_NODE $C_PID" >> "$PID_FILE"
sleep 1

ok "Distributed gRPC Cluster is UP and RUNNING!"
echo -e "    Coordinator Address: ${BOLD}${COORD_NODE}:${COORD_PORT}${RESET}"
echo -e "    Workers Attached   : ${WORKER_ADDR_STR}"

if [ "$DEMO_MODE" -eq 1 ]; then
    section "Executing Multi-Node Streaming Demo"
    DEMO_DATASET="/tmp/rce_cluster_demo_10k.txt"
    info "Generating demo dataset (10,000 records)..."
    "$PYTHON_CMD" hw3_grpc/dataset/generate_dataset.py -n 10000 -k 10 -s 50 -o "$DEMO_DATASET" --seed 42

    info "Streaming dataset to Coordinator (${COORD_NODE}:${COORD_PORT})..."
    "$PYTHON_CMD" -m hw3_grpc.client.streaming_client \
        --host "$COORD_NODE" \
        --port "$COORD_PORT" \
        --dataset "$DEMO_DATASET" \
        --batch-size 500

    info "Querying live analytics from Coordinator (${COORD_NODE}:${COORD_PORT})..."
    "$PYTHON_CMD" -m hw3_grpc.client.query_client \
        --host "$COORD_NODE" \
        --port "$COORD_PORT"
    ok "Multi-node demonstration completed successfully."
else
    echo ""
    echo -e "${BOLD}${CYAN}To interact with the cluster from any compute node:${RESET}"
    echo "  1. Stream records:"
    echo "     python3 -m hw3_grpc.client.streaming_client --host $COORD_NODE --port $COORD_PORT --dataset <file>"
    echo "  2. Query analytics:"
    echo "     python3 -m hw3_grpc.client.query_client --host $COORD_NODE --port $COORD_PORT"
    echo "  3. Live terminal dashboard:"
    echo "     python3 -m hw3_grpc.dashboard.dashboard --host $COORD_NODE --port $COORD_PORT"
    echo ""
    echo "Press [Ctrl+C] to stop all cluster processes."
    
    # Wait for Ctrl+C
    while true; do
        sleep 2
    done
fi
