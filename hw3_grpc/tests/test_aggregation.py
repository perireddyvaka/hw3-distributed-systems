"""hw3_grpc/tests/test_aggregation.py — Unit tests for merge/aggregation logic."""

import pytest
from hw3_grpc.common.analytics import AnalyticsAccumulator
from hw3_grpc.common.aggregation import merge, merge_two
from hw3_grpc.common.models import WeatherRecord


def make_rec(ts=1600000000, st=0, t=25.0, h=60.0, p=1013.0, r=10.0, w=20.0):
    return WeatherRecord(timestamp=ts, station_id=st, temperature=t,
                         humidity=h, pressure=p, rainfall=r, wind_speed=w)


def accum_from_records(records, k=10):
    acc = AnalyticsAccumulator(k=k)
    for r in records:
        acc.add(r)
    return acc.snapshot()


class TestMerge:
    def _records(self):
        return [
            make_rec(ts=1600000000, st=0, t=10.0, h=20.0, p=900.0, r=5.0, w=10.0),
            make_rec(ts=1600000060, st=1, t=30.0, h=80.0, p=1100.0, r=15.0, w=30.0),
            make_rec(ts=1600000120, st=2, t=50.0, h=50.0, p=1000.0, r=0.0, w=0.0),
            make_rec(ts=1600000180, st=0, t=-5.0, h=40.0, p=950.0, r=20.0, w=5.0),
            make_rec(ts=1600000240, st=1, t=25.0, h=60.0, p=1013.0, r=10.0, w=15.0),
            make_rec(ts=1600000300, st=2, t=20.0, h=70.0, p=1005.0, r=8.0, w=25.0),
        ]

    def test_merge_equals_sequential(self):
        records = self._records()
        # Sequential (reference)
        ref = accum_from_records(records, k=3)

        # Split: worker0 gets even indices, worker1 gets odd indices
        w0 = accum_from_records(records[::2], k=3)
        w1 = accum_from_records(records[1::2], k=3)
        merged = merge([w0, w1])

        assert merged.total_measurements == ref.total_measurements
        assert abs(merged.avg_temperature - ref.avg_temperature) < 1e-9
        assert merged.min_temperature == ref.min_temperature
        assert merged.max_temperature == ref.max_temperature
        assert abs(merged.total_rainfall - ref.total_rainfall) < 1e-9
        assert merged.extreme_temperature_events == ref.extreme_temperature_events

    def test_merge_hottest_consistent(self):
        records = self._records()
        ref = accum_from_records(records, k=3)
        w0 = accum_from_records(records[:3], k=3)
        w1 = accum_from_records(records[3:], k=3)
        merged = merge([w0, w1])
        assert merged.hottest.temperature == ref.hottest.temperature
        assert merged.hottest.timestamp == ref.hottest.timestamp
        assert merged.hottest.station_id == ref.hottest.station_id

    def test_merge_coldest_consistent(self):
        records = self._records()
        ref = accum_from_records(records, k=3)
        w0 = accum_from_records(records[:3], k=3)
        w1 = accum_from_records(records[3:], k=3)
        merged = merge([w0, w1])
        assert merged.coldest.temperature == ref.coldest.temperature

    def test_merge_busiest_interval(self):
        records = self._records()
        ref = accum_from_records(records, k=3)
        w0 = accum_from_records(records[::2], k=3)
        w1 = accum_from_records(records[1::2], k=3)
        merged = merge([w0, w1])
        assert merged.busiest_interval == ref.busiest_interval
        assert merged.busiest_interval_count == ref.busiest_interval_count

    def test_merge_station_stats(self):
        records = self._records()
        ref = accum_from_records(records, k=3)
        w0 = accum_from_records(records[::2], k=3)
        w1 = accum_from_records(records[1::2], k=3)
        merged = merge([w0, w1])
        for sid in ref.station_stats:
            assert sid in merged.station_stats
            assert merged.station_stats[sid].count == ref.station_stats[sid].count

    def test_merge_empty_snapshots(self):
        empty = AnalyticsAccumulator(k=5).snapshot()
        result = merge([empty, empty])
        assert result.total_measurements == 0

    def test_merge_four_workers(self):
        records = [make_rec(ts=1600000000 + i * 60, st=i % 5, t=float(i)) for i in range(20)]
        ref = accum_from_records(records, k=5)
        # Split into 4 chunks
        chunks = [records[i::4] for i in range(4)]
        snaps = [accum_from_records(c, k=5) for c in chunks]
        merged = merge(snaps)
        assert merged.total_measurements == ref.total_measurements
        assert abs(merged.avg_temperature - ref.avg_temperature) < 1e-9

    def test_merge_two_convenience(self):
        records = self._records()
        w0 = accum_from_records(records[:3], k=3)
        w1 = accum_from_records(records[3:], k=3)
        m1 = merge([w0, w1])
        m2 = merge_two(w0, w1)
        assert m1.total_measurements == m2.total_measurements
