"""hw3_grpc/common/aggregation.py — Merge worker-local analytics states.

Given N worker AnalyticsSnapshot objects (each computed on a disjoint subset
of records), merge() produces a single AnalyticsSnapshot that is numerically
identical to processing ALL records sequentially.

This is the distributed-systems equivalent of MPI_Reduce in HW2.
"""

from __future__ import annotations
from typing import Iterable

from hw3_grpc.common.models import AnalyticsSnapshot, MeasurementRef, StationStat
from hw3_grpc.common.analytics import _hottest_beats, _coldest_beats


def merge(snapshots: Iterable[AnalyticsSnapshot]) -> AnalyticsSnapshot:
    """Merge multiple AnalyticsSnapshots into one global snapshot.

    The result is equivalent to having processed all records sequentially.
    Order of snapshots does not matter (the operation is commutative).
    """
    snaps = list(snapshots)
    if not snaps:
        return AnalyticsSnapshot()

    # Use the first snapshot's k value
    k = snaps[0].k

    # Accumulators
    total = 0
    sum_t = sum_h = sum_p = sum_rain = sum_wind = 0.0
    min_t = min_h = min_p = 1e18
    max_t = max_rain = max_wind = -1e18
    max_h = max_p = -1e18
    extreme_count = 0
    hottest = MeasurementRef(0, 0, -1e18)
    coldest = MeasurementRef(0, 0, 1e18)
    interval_counts: dict = {}
    station_stats: dict = {}

    for snap in snaps:
        if snap.total_measurements == 0:
            continue

        total += snap.total_measurements
        sum_t += snap.sum_temperature
        sum_h += snap.sum_humidity
        sum_p += snap.sum_pressure
        sum_rain += snap.total_rainfall
        sum_wind += snap.sum_wind_speed

        if snap.min_temperature < min_t:
            min_t = snap.min_temperature
        if snap.max_temperature > max_t:
            max_t = snap.max_temperature
        if snap.min_humidity < min_h:
            min_h = snap.min_humidity
        if snap.max_humidity > max_h:
            max_h = snap.max_humidity
        if snap.min_pressure < min_p:
            min_p = snap.min_pressure
        if snap.max_pressure > max_p:
            max_p = snap.max_pressure
        if snap.max_rainfall > max_rain:
            max_rain = snap.max_rainfall
        if snap.max_wind_speed > max_wind:
            max_wind = snap.max_wind_speed

        extreme_count += snap.extreme_temperature_events

        # hottest/coldest with tie-breaking
        cand_h = snap.hottest
        if cand_h.temperature > -1e17 and _hottest_beats(cand_h, hottest):
            hottest = MeasurementRef(cand_h.timestamp, cand_h.station_id, cand_h.temperature)

        cand_c = snap.coldest
        if cand_c.temperature < 1e17 and _coldest_beats(cand_c, coldest):
            coldest = MeasurementRef(cand_c.timestamp, cand_c.station_id, cand_c.temperature)

        # merge interval counts
        for interval, count in snap.interval_counts.items():
            interval_counts[interval] = interval_counts.get(interval, 0) + count

        # merge station stats
        for sid, st in snap.station_stats.items():
            if sid not in station_stats:
                station_stats[sid] = StationStat(station_id=sid)
            gs = station_stats[sid]
            gs.count += st.count
            gs.sum_temperature += st.sum_temperature
            gs.sum_rainfall += st.sum_rainfall

    result = AnalyticsSnapshot(
        total_measurements=total,
        sum_temperature=sum_t,
        min_temperature=min_t,
        max_temperature=max_t,
        sum_humidity=sum_h,
        min_humidity=min_h,
        max_humidity=max_h,
        sum_pressure=sum_p,
        min_pressure=min_p,
        max_pressure=max_p,
        total_rainfall=sum_rain,
        max_rainfall=max_rain,
        sum_wind_speed=sum_wind,
        max_wind_speed=max_wind,
        extreme_temperature_events=extreme_count,
        hottest=hottest,
        coldest=coldest,
        interval_counts=interval_counts,
        station_stats=station_stats,
        k=k,
    )
    return result


def merge_two(a: AnalyticsSnapshot, b: AnalyticsSnapshot) -> AnalyticsSnapshot:
    """Convenience wrapper to merge exactly two snapshots."""
    return merge([a, b])
