"""hw3_grpc/coordinator/server.py — gRPC Coordinator service.

The coordinator is the central hub of the HW3 system:
  - Accepts a streaming ingestion of WeatherRecord batches from the streaming client.
  - Dispatches records round-robin to N workers.
  - Periodically collects and merges worker states into a global snapshot.
  - Serves analytics queries from query clients and dashboard.

Usage:
    python -m hw3_grpc.coordinator.server \\
        --workers 4 --port 50050 [--worker-base-port 50060] [--k 10]
"""

from __future__ import annotations
import argparse
import logging
import signal
import sys
import threading
from concurrent import futures
from typing import Iterator

import grpc

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.common.models import AnalyticsSnapshot
from hw3_grpc.coordinator.dispatcher import Dispatcher
from hw3_grpc.coordinator.state import GlobalAnalyticsState

log = logging.getLogger("coordinator")


def _snapshot_to_proto(snap: AnalyticsSnapshot, k: int) -> weather_pb2.AnalyticsSnapshot:
    """Convert an AnalyticsSnapshot to its proto representation."""
    top = snap.top_k_stations()
    proto_stations = [
        weather_pb2.StationStat(
            station_id=s.station_id,
            count=s.count,
            avg_temperature=s.avg_temperature,
            total_rainfall=s.total_rainfall,
        )
        for s in top
    ]
    bi = snap.busiest_interval
    n = snap.total_measurements
    proto = weather_pb2.AnalyticsSnapshot(
        total_measurements=n,
        avg_temperature=snap.avg_temperature,
        min_temperature=snap.min_temperature if n > 0 else 0.0,
        max_temperature=snap.max_temperature if n > 0 else 0.0,
        avg_humidity=snap.avg_humidity,
        min_humidity=snap.min_humidity if n > 0 else 0.0,
        max_humidity=snap.max_humidity if n > 0 else 0.0,
        avg_pressure=snap.avg_pressure,
        min_pressure=snap.min_pressure if n > 0 else 0.0,
        max_pressure=snap.max_pressure if n > 0 else 0.0,
        total_rainfall=snap.total_rainfall,
        max_rainfall=snap.max_rainfall if n > 0 else 0.0,
        avg_wind_speed=snap.avg_wind_speed,
        max_wind_speed=snap.max_wind_speed if n > 0 else 0.0,
        extreme_temperature_events=snap.extreme_temperature_events,
        hottest=weather_pb2.MeasurementRef(
            timestamp=snap.hottest.timestamp if n > 0 else 0,
            station_id=snap.hottest.station_id if n > 0 else 0,
            temperature=snap.hottest.temperature if n > 0 else 0.0,
        ),
        coldest=weather_pb2.MeasurementRef(
            timestamp=snap.coldest.timestamp if n > 0 else 0,
            station_id=snap.coldest.station_id if n > 0 else 0,
            temperature=snap.coldest.temperature if n > 0 else 0.0,
        ),
        busiest_interval=bi if bi is not None else -1,
        busiest_interval_count=snap.busiest_interval_count,
        top_stations=proto_stations,
        k=k,
    )
    return proto


class CoordinatorServicer(weather_pb2_grpc.CoordinatorServiceServicer):
    """gRPC servicer implementing CoordinatorService."""

    def __init__(self, dispatcher: Dispatcher, global_state: GlobalAnalyticsState,
                 k: int = 10) -> None:
        self._dispatcher = dispatcher
        self._global_state = global_state
        self._k = k
        self._total_received: int = 0
        self._stream_lock = threading.Lock()

    def StreamMeasurements(
        self, request_iterator: Iterator[weather_pb2.RecordBatch], context
    ) -> weather_pb2.StreamResponse:
        """Receive streaming record batches from the client."""
        with self._stream_lock:
            if self._global_state.is_stream_done:
                self._global_state.reset()
                self._dispatcher.reset()
                self._total_received = 0
        received = 0
        try:
            for batch in request_iterator:
                if not context.is_active():
                    break
                self._dispatcher.dispatch(batch)
                received += len(batch.records)
                with self._stream_lock:
                    self._total_received += len(batch.records)

            # Stream ended — wait for all in-flight dispatches to land before reading state
            log.info("Stream complete. Received %d records. Flushing dispatch pipeline...", received)
            self._dispatcher.flush()
            log.info("Dispatch pipeline flushed. Collecting final state...")
            final_snap = self._dispatcher.collect_all_states(k=self._k)
            self._global_state.update(final_snap)
            self._global_state.mark_stream_done()
            log.info("Final analytics state updated (total=%d).", final_snap.total_measurements)

            return weather_pb2.StreamResponse(
                success=True,
                records_received=received,
                message=f"Processed {received} records.",
            )
        except Exception as exc:
            log.error("Error during streaming: %s", exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return weather_pb2.StreamResponse(success=False, records_received=received, message=str(exc))

    def GetAnalytics(
        self, request: weather_pb2.AnalyticsRequest, context
    ) -> weather_pb2.AnalyticsSnapshot:
        """Return current or final analytics snapshot."""
        if request.final_only and not self._global_state.is_stream_done:
            # Best-effort: return current state even if not final
            log.debug("GetAnalytics(final_only=True) called before stream done; returning current state.")

        # Trigger a fresh aggregation on every query (for accuracy)
        # For high-frequency dashboards, we could cache; for correctness we aggregate now.
        try:
            fresh_snap = self._dispatcher.collect_all_states(k=self._k)
            self._global_state.update(fresh_snap)
        except Exception as exc:
            log.warning("Could not refresh state from workers: %s; returning cached.", exc)

        snap = self._global_state.get_snapshot()
        return _snapshot_to_proto(snap, self._k)


def serve(
    num_workers: int,
    port: int,
    worker_base_port: int,
    worker_host: str,
    k: int = 10,
    worker_addresses: Optional[List[str]] = None,
) -> None:
    if not worker_addresses:
        worker_addresses = [
            f"{worker_host}:{worker_base_port + i}" for i in range(num_workers)
        ]
    log.info("Coordinator starting with %d workers: %s", len(worker_addresses), worker_addresses)

    dispatcher = Dispatcher(worker_addresses)
    global_state = GlobalAnalyticsState()

    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=cfg.GRPC_OPTIONS,
    )
    servicer = CoordinatorServicer(dispatcher, global_state, k=k)
    weather_pb2_grpc.add_CoordinatorServiceServicer_to_server(servicer, grpc_server)
    bind_addr = f"[::]:{port}"
    grpc_server.add_insecure_port(bind_addr)
    grpc_server.start()
    log.info("Coordinator listening on port %d", port)

    def _handle_signal(sig, frame):
        log.info("Coordinator shutting down ...")
        dispatcher.close()
        grpc_server.stop(grace=3)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    grpc_server.wait_for_termination()


def main() -> None:
    parser = argparse.ArgumentParser(description="HW3 gRPC Coordinator")
    parser.add_argument("--workers", type=int, default=cfg.WORKER_COUNT)
    parser.add_argument("--port", type=int, default=cfg.COORDINATOR_PORT)
    parser.add_argument("--worker-base-port", type=int, default=cfg.WORKER_BASE_PORT)
    parser.add_argument("--worker-host", type=str, default=cfg.COORDINATOR_HOST)
    parser.add_argument(
        "--worker-addresses",
        type=str,
        default=None,
        help="Comma-separated list of worker host:port addresses (e.g. node02:50060,node03:50060)",
    )
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    worker_addrs = None
    if args.worker_addresses:
        worker_addrs = [addr.strip() for addr in args.worker_addresses.split(",") if addr.strip()]

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [Coordinator] %(levelname)s %(message)s",
    )
    serve(
        num_workers=args.workers if not worker_addrs else len(worker_addrs),
        port=args.port,
        worker_base_port=args.worker_base_port,
        worker_host=args.worker_host,
        k=args.k,
        worker_addresses=worker_addrs,
    )


if __name__ == "__main__":
    main()
