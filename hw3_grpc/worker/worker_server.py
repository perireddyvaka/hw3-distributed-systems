"""hw3_grpc/worker/worker_server.py — gRPC worker service.

Each worker runs as an independent process, receives record batches from the
coordinator, processes them locally, and exposes its state for coordinator
aggregation.

Usage:
    python -m hw3_grpc.worker.worker_server --id 0 --port 50060 [--k 10]
"""

from __future__ import annotations
import argparse
import logging
import signal
import sys
import time
from concurrent import futures

import grpc

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.common.models import WeatherRecord
from hw3_grpc.worker.worker_state import WorkerLocalState

log = logging.getLogger("worker")


class WorkerServicer(weather_pb2_grpc.WorkerServiceServicer):
    """gRPC servicer implementing WorkerService."""

    def __init__(self, worker_id: int, k: int = 10) -> None:
        self._state = WorkerLocalState(worker_id=worker_id, k=k)
        self._worker_id = worker_id

    def ProcessBatch(self, request: weather_pb2.RecordBatch, context) -> weather_pb2.BatchAck:
        """Receive a batch of records from the coordinator and process them."""
        records = [
            WeatherRecord(
                timestamp=r.timestamp,
                station_id=r.station_id,
                temperature=r.temperature,
                humidity=r.humidity,
                pressure=r.pressure,
                rainfall=r.rainfall,
                wind_speed=r.wind_speed,
            )
            for r in request.records
        ]
        self._state.process_records(records)
        log.debug(
            "Worker %d processed batch %d (%d records, total=%d)",
            self._worker_id, request.batch_index, len(records), self._state.count,
        )
        return weather_pb2.BatchAck(
            success=True,
            worker_id=self._worker_id,
            count=self._state.count,
        )

    def GetWorkerState(self, request: weather_pb2.StateRequest, context) -> weather_pb2.WorkerAnalyticsState:
        """Return the current local analytics state to the coordinator."""
        snap = self._state.get_snapshot()
        proto_state = weather_pb2.WorkerAnalyticsState(
            worker_id=self._worker_id,
            count=snap.total_measurements,
            sum_temperature=snap.sum_temperature,
            min_temperature=snap.min_temperature,
            max_temperature=snap.max_temperature,
            sum_humidity=snap.sum_humidity,
            min_humidity=snap.min_humidity,
            max_humidity=snap.max_humidity,
            sum_pressure=snap.sum_pressure,
            min_pressure=snap.min_pressure,
            max_pressure=snap.max_pressure,
            total_rainfall=snap.total_rainfall,
            max_rainfall=snap.max_rainfall,
            sum_wind_speed=snap.sum_wind_speed,
            max_wind_speed=snap.max_wind_speed,
            extreme_temperature_events=snap.extreme_temperature_events,
            hottest=weather_pb2.MeasurementRef(
                timestamp=snap.hottest.timestamp,
                station_id=snap.hottest.station_id,
                temperature=snap.hottest.temperature,
            ),
            coldest=weather_pb2.MeasurementRef(
                timestamp=snap.coldest.timestamp,
                station_id=snap.coldest.station_id,
                temperature=snap.coldest.temperature,
            ),
            interval_keys=list(snap.interval_counts.keys()),
            interval_values=list(snap.interval_counts.values()),
            station_ids=[s.station_id for s in snap.station_stats.values()],
            station_counts=[s.count for s in snap.station_stats.values()],
            station_sum_temp=[s.sum_temperature for s in snap.station_stats.values()],
            station_sum_rain=[s.sum_rainfall for s in snap.station_stats.values()],
            k=snap.k,
        )
        return proto_state


def serve(worker_id: int, port: int, k: int = 10) -> None:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=cfg.GRPC_OPTIONS,
    )
    servicer = WorkerServicer(worker_id=worker_id, k=k)
    weather_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    bind_addr = f"[::]:{port}"
    server.add_insecure_port(bind_addr)
    server.start()
    log.info("Worker %d started on port %d", worker_id, port)

    def _handle_signal(sig, frame):
        log.info("Worker %d shutting down ...", worker_id)
        server.stop(grace=2)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    server.wait_for_termination()


def main() -> None:
    parser = argparse.ArgumentParser(description="HW3 gRPC Worker")
    parser.add_argument("--id", type=int, required=True, help="Worker ID (0-based)")
    parser.add_argument("--port", type=int, required=True, help="Listen port")
    parser.add_argument("--k", type=int, default=10, help="Top-K stations parameter")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [Worker-%(name)s] %(levelname)s %(message)s",
    )
    serve(worker_id=args.id, port=args.port, k=args.k)


if __name__ == "__main__":
    main()
