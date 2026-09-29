"""hw3_grpc/coordinator/dispatcher.py — Round-robin record dispatcher.

Distributes incoming RecordBatch messages from the streaming client to workers
using round-robin assignment, then collects worker state for global aggregation.
"""

from __future__ import annotations
import logging
import sys
from typing import List

import grpc

sys.path.insert(0, "hw3_grpc/generated")
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
    """Round-robin dispatcher to N worker gRPC stubs."""

    def __init__(self, worker_addresses: List[str]) -> None:
        if not worker_addresses:
            raise ValueError("At least one worker address is required.")
        self._addresses = worker_addresses
        self._n = len(worker_addresses)
        self._stubs: List[weather_pb2_grpc.WorkerServiceStub] = []
        self._channels: List[grpc.Channel] = []
        self._batch_counter: int = 0

        for addr in worker_addresses:
            ch = grpc.insecure_channel(addr, options=cfg.GRPC_OPTIONS)
            self._channels.append(ch)
            self._stubs.append(weather_pb2_grpc.WorkerServiceStub(ch))
        log.info("Dispatcher connected to %d workers: %s", self._n, worker_addresses)

    def dispatch(self, batch: weather_pb2.RecordBatch) -> None:
        """Forward batch to the next worker in round-robin order."""
        worker_idx = self._batch_counter % self._n
        self._batch_counter += 1
        try:
            ack = self._stubs[worker_idx].ProcessBatch(batch, timeout=30)
            log.debug("Dispatched batch %d to worker %d (ack count=%d)",
                      batch.batch_index, worker_idx, ack.count)
        except grpc.RpcError as e:
            log.error("Failed to dispatch to worker %d: %s", worker_idx, e)
            raise

    def collect_all_states(self, k: int = 10) -> AnalyticsSnapshot:
        """Fetch state from every worker and merge into a global snapshot."""
        snapshots = []
        req = weather_pb2.StateRequest(include_all=True)
        for i, stub in enumerate(self._stubs):
            try:
                ws = stub.GetWorkerState(req, timeout=30)
                ws.k = k
                snap = _proto_state_to_snapshot(ws)
                snapshots.append(snap)
                log.debug("Collected state from worker %d (count=%d)", i, snap.total_measurements)
            except grpc.RpcError as e:
                log.error("Failed to get state from worker %d: %s", i, e)
                raise
        return merge(snapshots)

    def close(self) -> None:
        for ch in self._channels:
            ch.close()
