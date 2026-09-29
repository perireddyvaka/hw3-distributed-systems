"""hw3_grpc/common/analytics.py — Incremental analytics engine for HW3.

This module implements the same analytics as the HW2 sequential reference
(hw2_mpi/src/q8_seq.cpp) using an incremental (streaming) approach.
No full dataset needs to be stored; only sufficient statistics are kept.

Analytics computed (matching HW2 output exactly):
  TOTAL_MEASUREMENTS
  AVERAGE/MIN/MAX TEMPERATURE, HUMIDITY, PRESSURE
  TOTAL/MAX RAINFALL
  AVERAGE/MAX WIND_SPEED
  EXTREME_TEMPERATURE_EVENTS  (temp >= 40.0 OR temp <= 0.0)
  HOTTEST_MEASUREMENT  (tie: lowest ts, then lowest station_id)
  COLDEST_MEASUREMENT  (tie: lowest ts, then lowest station_id)
  BUSIEST_INTERVAL     (60-second buckets, tie: lowest bucket index)
  TOP_K_STATIONS       (by count desc, station_id asc on tie)
"""

from __future__ import annotations
from copy import deepcopy
from typing import Dict

from hw3_grpc.common.models import (
    AnalyticsSnapshot,
    MeasurementRef,
    StationStat,
    WeatherRecord,
)


def _hottest_beats(candidate: MeasurementRef, current: MeasurementRef) -> bool:
    """Return True if candidate should replace current as hottest."""
    if candidate.temperature != current.temperature:
        return candidate.temperature > current.temperature
    if candidate.timestamp != current.timestamp:
        return candidate.timestamp < current.timestamp
    return candidate.station_id < current.station_id


def _coldest_beats(candidate: MeasurementRef, current: MeasurementRef) -> bool:
    """Return True if candidate should replace current as coldest."""
    if candidate.temperature != current.temperature:
        return candidate.temperature < current.temperature
    if candidate.timestamp != current.timestamp:
        return candidate.timestamp < current.timestamp
    return candidate.station_id < current.station_id


class AnalyticsAccumulator:
    """Incremental analytics accumulator.

    Call add(record) for each incoming WeatherRecord.
    Call snapshot() to get the current AnalyticsSnapshot.
    """

    def __init__(self, k: int = 10) -> None:
        self._k = k
        self._count: int = 0
        # temperature
        self._sum_t: float = 0.0
        self._min_t: float = 1e18
        self._max_t: float = -1e18
        # humidity
        self._sum_h: float = 0.0
        self._min_h: float = 1e18
        self._max_h: float = -1e18
        # pressure
        self._sum_p: float = 0.0
        self._min_p: float = 1e18
        self._max_p: float = -1e18
        # rainfall / wind
        self._total_rain: float = 0.0
        self._max_rain: float = -1e18
        self._sum_wind: float = 0.0
        self._max_wind: float = -1e18
        # extremes
        self._extreme_count: int = 0
        self._hottest = MeasurementRef(0, 0, -1e18)
        self._coldest = MeasurementRef(0, 0, 1e18)
        # interval
        self._interval_counts: Dict[int, int] = {}
        # station
        self._station_stats: Dict[int, StationStat] = {}

    def add(self, rec: WeatherRecord) -> None:
        """Process a single WeatherRecord and update all statistics."""
        self._count += 1

        t, h, p, r, w = (
            rec.temperature, rec.humidity, rec.pressure,
            rec.rainfall, rec.wind_speed,
        )

        # sums
        self._sum_t += t
        self._sum_h += h
        self._sum_p += p
        self._total_rain += r
        self._sum_wind += w

        # min/max
        if t < self._min_t:
            self._min_t = t
        if t > self._max_t:
            self._max_t = t
        if h < self._min_h:
            self._min_h = h
        if h > self._max_h:
            self._max_h = h
        if p < self._min_p:
            self._min_p = p
        if p > self._max_p:
            self._max_p = p
        if r > self._max_rain:
            self._max_rain = r
        if w > self._max_wind:
            self._max_wind = w

        # extreme temperature
        if t >= 40.0 or t <= 0.0:
            self._extreme_count += 1

        # hottest / coldest
        cand = MeasurementRef(rec.timestamp, rec.station_id, t)
        if _hottest_beats(cand, self._hottest):
            self._hottest = cand
        if _coldest_beats(cand, self._coldest):
            self._coldest = cand

        # busiest interval (60-second buckets)
        bucket = rec.timestamp // 60
        self._interval_counts[bucket] = self._interval_counts.get(bucket, 0) + 1

        # station stats
        sid = rec.station_id
        if sid not in self._station_stats:
            self._station_stats[sid] = StationStat(station_id=sid)
        st = self._station_stats[sid]
        st.count += 1
        st.sum_temperature += t
        st.sum_rainfall += r

    def snapshot(self) -> AnalyticsSnapshot:
        """Return a deep-copy snapshot of the current analytics state."""
        snap = AnalyticsSnapshot(
            total_measurements=self._count,
            sum_temperature=self._sum_t,
            min_temperature=self._min_t,
            max_temperature=self._max_t,
            sum_humidity=self._sum_h,
            min_humidity=self._min_h,
            max_humidity=self._max_h,
            sum_pressure=self._sum_p,
            min_pressure=self._min_p,
            max_pressure=self._max_p,
            total_rainfall=self._total_rain,
            max_rainfall=self._max_rain,
            sum_wind_speed=self._sum_wind,
            max_wind_speed=self._max_wind,
            extreme_temperature_events=self._extreme_count,
            hottest=MeasurementRef(
                self._hottest.timestamp,
                self._hottest.station_id,
                self._hottest.temperature,
            ),
            coldest=MeasurementRef(
                self._coldest.timestamp,
                self._coldest.station_id,
                self._coldest.temperature,
            ),
            interval_counts=dict(self._interval_counts),
            station_stats={
                sid: StationStat(
                    station_id=s.station_id,
                    count=s.count,
                    sum_temperature=s.sum_temperature,
                    sum_rainfall=s.sum_rainfall,
                )
                for sid, s in self._station_stats.items()
            },
            k=self._k,
        )
        return snap

    @property
    def count(self) -> int:
        return self._count
