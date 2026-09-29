"""hw3_grpc/dashboard/dashboard.py — CLI live analytics dashboard.

Periodically queries the coordinator and displays live analytics while
the stream is running.

Usage:
    python -m hw3_grpc.dashboard.dashboard \\
        [--host localhost] [--port 50050] [--interval 1.0]
"""

from __future__ import annotations
import argparse
import sys
import time
import os

import grpc

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.client.query_client import query, format_snapshot


def clear() -> None:
    os.system("clear" if os.name == "posix" else "cls")


def dashboard_loop(
    host: str = cfg.COORDINATOR_HOST,
    port: int = cfg.COORDINATOR_PORT,
    interval: float = cfg.QUERY_INTERVAL,
) -> None:
    print(f"HW3 Real-Time Dashboard — querying {host}:{port} every {interval}s")
    print("Press Ctrl+C to exit.\n")
    prev_count = -1
    try:
        while True:
            try:
                snap = query(host=host, port=port)
                clear()
                print(f"[{time.strftime('%H:%M:%S')}] HW3 REAL-TIME WEATHER ANALYTICS DASHBOARD")
                print(f"Status: {'STREAMING' if snap.total_measurements != prev_count else 'IDLE'}")
                print(format_snapshot(snap))
                prev_count = snap.total_measurements
            except grpc.RpcError as e:
                print(f"[{time.strftime('%H:%M:%S')}] Coordinator not available: {e.details()}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="HW3 Live Analytics Dashboard")
    parser.add_argument("--host", default=cfg.COORDINATOR_HOST)
    parser.add_argument("--port", type=int, default=cfg.COORDINATOR_PORT)
    parser.add_argument("--interval", type=float, default=cfg.QUERY_INTERVAL,
                        help="Refresh interval in seconds")
    args = parser.parse_args()
    dashboard_loop(host=args.host, port=args.port, interval=args.interval)


if __name__ == "__main__":
    main()
