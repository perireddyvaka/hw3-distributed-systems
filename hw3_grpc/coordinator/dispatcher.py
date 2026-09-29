"""hw3_grpc/coordinator/dispatcher.py — Round-robin record dispatcher.

Distributes incoming RecordBatch messages from the streaming client to workers
using round-robin assignment, then collects worker state for global aggregation.

KEY DESIGN:
  - dispatch()          : fire-and-forget to a thread pool — the coordinator
                          does NOT block waiting for a worker ACK. The next
                          batch is submitted immediately to the next worker.
                          This gives true pipeline parallelism across workers.
  - flush()             : called once after the stream ends; waits for all
                          in-flight dispatches to complete before aggregating.
  - collect_all_states(): fans out GetWorkerState() to ALL workers in parallel
                          via a ThreadPoolExecutor, so query latency = 1×
                          worker latency (not N× sequential).
"""

from __future__ import annotations
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, Future, wait as futures_wait, ALL_COMPLETED
from typing import List

import grpc

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.common.aggregation import merge
from hw3_grpc.common.models import AnalyticsSnapshot, MeasurementRef, StationStat

log = logging.getLogger("coordinator.dispatcher")


def _proto_state_to_snapshot(ws: weather_pb2.WorkerAnalyticsState) -> AnalyticsSnapshot:
    """Convert a WorkerAnalyticsState proto message to an AnalyticsSnapshot."""
    interval_counts = dict(zip(ws.interval_keys, ws.interval_values))
    station_stats = {}
    for sid, cnt, st, sr in zip(
        ws.station_ids, ws.station_counts, ws.station_sum_temp, ws.station_sum_rain
    ):
        station_stats[sid] = StationStat(
            station_id=sid,
            count=cnt,
            sum_temperature=st,
            sum_rainfall=sr,
        )
    snap = AnalyticsSnapshot(
        total_measurements=ws.count,
        sum_temperature=ws.sum_temperature,
        min_temperature=ws.min_temperature,
        max_temperature=ws.max_temperature,
        sum_humidity=ws.sum_humidity,
        min_humidity=ws.min_humidity,
        max_humidity=ws.max_humidity,
        sum_pressure=ws.sum_pressure,
        min_pressure=ws.min_pressure,
        max_pressure=ws.max_pressure,
        total_rainfall=ws.total_rainfall,
        max_rainfall=ws.max_rainfall,
        sum_wind_speed=ws.sum_wind_speed,
        max_wind_speed=ws.max_wind_speed,
        extreme_temperature_events=ws.extreme_temperature_events,
        hottest=MeasurementRef(ws.hottest.timestamp, ws.hottest.station_id, ws.hottest.temperature),
        coldest=MeasurementRef(ws.coldest.timestamp, ws.coldest.station_id, ws.coldest.temperature),
        interval_counts=interval_counts,
        station_stats=station_stats,
        k=ws.k,
    )
    return snap


class Dispatcher:
    """Round-robin dispatcher to N worker gRPC stubs.

    Design decisions:
    - dispatch() submits to a thread pool and returns immediately.
      The coordinator loop can advance to the next batch right away,
      allowing multiple workers to process concurrently.
    - A bounded pending-futures list tracks in-flight RPCs so flush()
      can wait for all of them before final aggregation.
    - collect_all_states() fans out to all workers in parallel using
      a separate thread pool, keeping query latency at O(1 worker).
    """

    def __init__(self, worker_addresses: List[str]) -> None:
        if not worker_addresses:
            raise ValueError("At least one worker address is required.")
        self._addresses = worker_addresses
        self._n = len(worker_addresses)
        self._stubs: List[weather_pb2_grpc.WorkerServiceStub] = []
        self._channels: List[grpc.Channel] = []
        self._batch_counter: int = 0

        self._counter_lock = threading.Lock()

        # Thread pool for fire-and-forget dispatch — 2 threads per worker
        # so we can keep the pipeline full even under transient latency spikes.
        self._dispatch_pool = ThreadPoolExecutor(
            max_workers=max(self._n * 2, 8),
            thread_name_prefix="dispatch",
        )
        # Track pending dispatch futures for flush()
        self._pending: List[Future] = []
        self._pending_lock = threading.Lock()

        for addr in worker_addresses:
            ch = grpc.insecure_channel(addr, options=cfg.GRPC_OPTIONS)
            self._channels.append(ch)
            self._stubs.append(weather_pb2_grpc.WorkerServiceStub(ch))
        log.info("Dispatcher connected to %d workers: %s", self._n, worker_addresses)

    # ── Ingestion ──────────────────────────────────────────────────────────────

    def dispatch(self, batch: weather_pb2.RecordBatch) -> Future:
        """Submit batch to the next worker in round-robin order.

        Returns immediately without waiting for the worker ACK.
        The RPC runs in the background thread pool, so the coordinator
        loop can submit the next batch to the next worker straight away.
        """
        with self._counter_lock:
            worker_idx = self._batch_counter % self._n
            self._batch_counter += 1

        stub = self._stubs[worker_idx]

        def _send():
            ack = stub.ProcessBatch(batch, timeout=30)
            log.debug(
                "Dispatched batch %d to worker %d (ack count=%d)",
                batch.batch_index, worker_idx, ack.count,
            )
            return ack

        future = self._dispatch_pool.submit(_send)
        with self._pending_lock:
            self._pending.append(future)
        return future

    def flush(self) -> None:
        """Block until every in-flight dispatch RPC has completed.

        Must be called after the stream ends and before collect_all_states(),
        to guarantee that every record has been ingested by its worker before
        we read the worker states.
        """
        with self._pending_lock:
            pending = list(self._pending)
            self._pending.clear()

        if not pending:
            return

        log.info("Flushing %d in-flight dispatch RPCs ...", len(pending))
        done, not_done = futures_wait(pending, timeout=60, return_when=ALL_COMPLETED)
        for f in done:
            exc = f.exception()
            if exc:
                log.error("Dispatch RPC failed: %s", exc)
                raise exc
        if not_done:
            log.error("%d dispatch RPCs did not complete within timeout.", len(not_done))
            raise RuntimeError("Dispatch flush timed out.")
        log.info("All dispatches flushed successfully.")

    def reset(self) -> None:
        """Reset batch counter for round-robin assignment."""
        with self._counter_lock:
            self._batch_counter = 0
        with self._pending_lock:
            self._pending.clear()

    # ── Aggregation ────────────────────────────────────────────────────────────

    def collect_all_states(self, k: int = 10) -> AnalyticsSnapshot:
        """Fetch state from every worker in parallel and merge into a global snapshot.

        Uses a dedicated ThreadPoolExecutor to fan out GetWorkerState() RPCs
        concurrently. Query latency is O(max single-worker latency), not O(N workers).
        """
        req = weather_pb2.StateRequest(include_all=True)

        def _fetch(args):
            i, stub = args
            try:
                ws = stub.GetWorkerState(req, timeout=30)
                ws.k = k
                snap = _proto_state_to_snapshot(ws)
                log.debug("Collected state from worker %d (count=%d)", i, snap.total_measurements)
                return snap
            except grpc.RpcError as e:
                log.error("Failed to get state from worker %d: %s", i, e)
                raise

        with ThreadPoolExecutor(
            max_workers=self._n, thread_name_prefix="collect"
        ) as pool:
            snapshots = list(pool.map(_fetch, enumerate(self._stubs)))

        return merge(snapshots)

    def close(self) -> None:
        self._dispatch_pool.shutdown(wait=True)
        for ch in self._channels:
            ch.close()
