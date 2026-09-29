#!/bin/bash
# hw3_grpc/scripts/stop_system.sh
# Gracefully stop the HW3 gRPC system started by start_system.sh.
#
# Usage:
#   bash hw3_grpc/scripts/stop_system.sh

PID_DIR="/tmp/hw3_pids"

if [ ! -d "$PID_DIR" ]; then
    echo "[stop_system] No PID directory found at $PID_DIR. Is the system running?"
    exit 0
fi

echo "[stop_system] Stopping HW3 system..."

for pidfile in "$PID_DIR"/*.pid; do
    [ -f "$pidfile" ] || continue
    pid=$(cat "$pidfile")
    name=$(basename "$pidfile" .pid)
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" && echo "[stop_system] Stopped $name (PID $pid)"
    else
        echo "[stop_system] $name (PID $pid) was not running."
    fi
    rm -f "$pidfile"
done

rmdir "$PID_DIR" 2>/dev/null || true
echo "[stop_system] Done."
