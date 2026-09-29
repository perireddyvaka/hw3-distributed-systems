#!/bin/bash
# hw3_grpc/scripts/start_system.sh
# Start the HW3 gRPC system: coordinator + N workers.
#
# Usage:
#   bash hw3_grpc/scripts/start_system.sh [--workers N] [--port PORT] [--worker-base-port P] [--k K]
#
# Run from the repository root.

set -euo pipefail

WORKERS=4
COORD_PORT=50050
WORKER_BASE=50060
K=10
LOG_LEVEL=INFO

while [[ $# -gt 0 ]]; do
    case $1 in
        --workers) WORKERS="$2"; shift 2 ;;
        --port) COORD_PORT="$2"; shift 2 ;;
        --worker-base-port) WORKER_BASE="$2"; shift 2 ;;
        --k) K="$2"; shift 2 ;;
        --log-level) LOG_LEVEL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

PID_DIR="/tmp/hw3_pids"
mkdir -p "$PID_DIR"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "[start_system] Generating proto stubs..."
bash hw3_grpc/scripts/generate_proto.sh

echo "[start_system] Starting $WORKERS workers (base port: $WORKER_BASE)..."
for i in $(seq 0 $((WORKERS-1))); do
    PORT=$((WORKER_BASE + i))
    python3 -m hw3_grpc.worker.worker_server \
        --id "$i" --port "$PORT" --k "$K" --log-level "$LOG_LEVEL" \
        >> "/tmp/hw3_worker_${i}.log" 2>&1 &
    echo $! > "$PID_DIR/worker_${i}.pid"
    echo "[start_system] Worker $i started on port $PORT (PID $(cat $PID_DIR/worker_${i}.pid))"
done

sleep 1

echo "[start_system] Starting coordinator on port $COORD_PORT..."
python3 -m hw3_grpc.coordinator.server \
    --workers "$WORKERS" --port "$COORD_PORT" \
    --worker-base-port "$WORKER_BASE" --k "$K" --log-level "$LOG_LEVEL" \
    >> "/tmp/hw3_coordinator.log" 2>&1 &
echo $! > "$PID_DIR/coordinator.pid"
echo "[start_system] Coordinator started (PID $(cat $PID_DIR/coordinator.pid))"

sleep 1
echo ""
echo "[start_system] System is ready!"
echo "  Coordinator: localhost:$COORD_PORT"
echo "  Workers:     $WORKERS workers on ports $WORKER_BASE-$((WORKER_BASE+WORKERS-1))"
echo "  PID files:   $PID_DIR/"
echo ""
echo "  Stream a dataset:"
echo "    python3 -m hw3_grpc.client.streaming_client --dataset <file> --port $COORD_PORT"
echo "  Query analytics:"
echo "    python3 -m hw3_grpc.client.query_client --port $COORD_PORT"
echo "  Live dashboard:"
echo "    python3 -m hw3_grpc.dashboard.dashboard --port $COORD_PORT"
echo "  Stop system:"
echo "    bash hw3_grpc/scripts/stop_system.sh"
