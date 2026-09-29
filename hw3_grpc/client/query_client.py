"""hw3_grpc/client/query_client.py — One-shot analytics query client.

Usage:
    python -m hw3_grpc.client.query_client [--host localhost] [--port 50050]
"""

from __future__ import annotations
import argparse
import sys

import grpc

sys.path.insert(0, "hw3_grpc/generated")
import weather_pb2
import weather_pb2_grpc

from hw3_grpc.common import config as cfg


def query(
    host: str = cfg.COORDINATOR_HOST,
    port: int = cfg.COORDINATOR_PORT,
    final_only: bool = False,
) -> weather_pb2.AnalyticsSnapshot:
    """Query the coordinator and return the analytics snapshot proto."""
    address = f"{host}:{port}"
    channel = grpc.insecure_channel(address, options=cfg.GRPC_OPTIONS)
    stub = weather_pb2_grpc.CoordinatorServiceStub(channel)
    try:
        snap = stub.GetAnalytics(
            weather_pb2.AnalyticsRequest(final_only=final_only), timeout=10
        )
    finally:
        channel.close()
    return snap


def format_snapshot(snap: weather_pb2.AnalyticsSnapshot) -> str:
    lines = [
        "=" * 56,
        "  HW3 WEATHER ANALYTICS — CURRENT SNAPSHOT",
        "=" * 56,
        f"  TOTAL_MEASUREMENTS           {snap.total_measurements:>15,}",
        "",
        f"  AVERAGE_TEMPERATURE          {snap.avg_temperature:>15.6f}",
        f"  MIN_TEMPERATURE              {snap.min_temperature:>15.6f}",
        f"  MAX_TEMPERATURE              {snap.max_temperature:>15.6f}",
        "",
        f"  AVERAGE_HUMIDITY             {snap.avg_humidity:>15.6f}",
        f"  MIN_HUMIDITY                 {snap.min_humidity:>15.6f}",
        f"  MAX_HUMIDITY                 {snap.max_humidity:>15.6f}",
        "",
        f"  AVERAGE_PRESSURE             {snap.avg_pressure:>15.6f}",
        f"  MIN_PRESSURE                 {snap.min_pressure:>15.6f}",
        f"  MAX_PRESSURE                 {snap.max_pressure:>15.6f}",
        "",
        f"  TOTAL_RAINFALL               {snap.total_rainfall:>15.6f}",
        f"  MAX_RAINFALL                 {snap.max_rainfall:>15.6f}",
        "",
        f"  AVERAGE_WIND_SPEED           {snap.avg_wind_speed:>15.6f}",
        f"  MAX_WIND_SPEED               {snap.max_wind_speed:>15.6f}",
        "",
        f"  EXTREME_TEMPERATURE_EVENTS   {snap.extreme_temperature_events:>15,}",
        "",
        f"  HOTTEST  ts={snap.hottest.timestamp}  st={snap.hottest.station_id}  t={snap.hottest.temperature:.6f}",
        f"  COLDEST  ts={snap.coldest.timestamp}  st={snap.coldest.station_id}  t={snap.coldest.temperature:.6f}",
        "",
        f"  BUSIEST_INTERVAL  bucket={snap.busiest_interval}  count={snap.busiest_interval_count}",
        "",
        f"  TOP_{snap.k}_STATIONS:",
    ]
    for s in snap.top_stations:
        lines.append(
            f"    station={s.station_id:>5}  count={s.count:>8,}  avg_t={s.avg_temperature:.6f}  rain={s.total_rainfall:.6f}"
        )
    lines.append("=" * 56)
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="HW3 Analytics Query Client")
    parser.add_argument("--host", default=cfg.COORDINATOR_HOST)
    parser.add_argument("--port", type=int, default=cfg.COORDINATOR_PORT)
    parser.add_argument("--final", action="store_true", help="Request final analytics only")
    args = parser.parse_args()

    snap = query(host=args.host, port=args.port, final_only=args.final)
    print(format_snapshot(snap))


if __name__ == "__main__":
    main()
