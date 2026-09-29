"""hw3_grpc/tests/test_analytics.py — Unit tests for the analytics engine."""

import pytest
from hw3_grpc.common.analytics import AnalyticsAccumulator, _hottest_beats, _coldest_beats
from hw3_grpc.common.models import WeatherRecord, MeasurementRef


def make_rec(ts=1600000000, st=0, t=25.0, h=60.0, p=1013.0, r=10.0, w=20.0):
    return WeatherRecord(timestamp=ts, station_id=st, temperature=t,
                         humidity=h, pressure=p, rainfall=r, wind_speed=w)


# ── Tie-breaking helpers ──────────────────────────────────────────────────────

class TestTieBreaking:
    def test_hottest_by_temperature(self):
        a = MeasurementRef(1000, 0, 45.0)
        b = MeasurementRef(1000, 0, 44.0)
        assert _hottest_beats(a, b)
        assert not _hottest_beats(b, a)

    def test_hottest_tie_lower_ts_wins(self):
        a = MeasurementRef(900, 0, 45.0)
        b = MeasurementRef(1000, 0, 45.0)
        assert _hottest_beats(a, b)   # lower ts wins
        assert not _hottest_beats(b, a)

    def test_hottest_tie_lower_station_wins(self):
        a = MeasurementRef(1000, 3, 45.0)
        b = MeasurementRef(1000, 5, 45.0)
        assert _hottest_beats(a, b)
        assert not _hottest_beats(b, a)

    def test_coldest_by_temperature(self):
        a = MeasurementRef(1000, 0, -5.0)
        b = MeasurementRef(1000, 0, 0.0)
        assert _coldest_beats(a, b)
        assert not _coldest_beats(b, a)

    def test_coldest_tie_lower_ts_wins(self):
        a = MeasurementRef(800, 0, -5.0)
        b = MeasurementRef(900, 0, -5.0)
        assert _coldest_beats(a, b)


# ── AnalyticsAccumulator ──────────────────────────────────────────────────────

class TestAccumulator:
    def test_empty(self):
        acc = AnalyticsAccumulator(k=5)
        snap = acc.snapshot()
        assert snap.total_measurements == 0

    def test_single_record(self):
        acc = AnalyticsAccumulator(k=5)
        acc.add(make_rec(t=30.0, h=70.0, p=1010.0, r=5.0, w=15.0))
        snap = acc.snapshot()
        assert snap.total_measurements == 1
        assert abs(snap.avg_temperature - 30.0) < 1e-9
        assert abs(snap.min_temperature - 30.0) < 1e-9
        assert abs(snap.max_temperature - 30.0) < 1e-9
        assert abs(snap.avg_humidity - 70.0) < 1e-9
        assert abs(snap.total_rainfall - 5.0) < 1e-9
        assert abs(snap.avg_wind_speed - 15.0) < 1e-9

    def test_multiple_records_averages(self):
        acc = AnalyticsAccumulator(k=5)
        acc.add(make_rec(t=10.0))
        acc.add(make_rec(t=20.0))
        acc.add(make_rec(t=30.0))
        snap = acc.snapshot()
        assert snap.total_measurements == 3
        assert abs(snap.avg_temperature - 20.0) < 1e-9
        assert snap.min_temperature == 10.0
        assert snap.max_temperature == 30.0

    def test_extreme_temperature_events(self):
        acc = AnalyticsAccumulator()
        acc.add(make_rec(t=40.0))   # boundary: >= 40 → extreme
        acc.add(make_rec(t=0.0))    # boundary: <= 0 → extreme
        acc.add(make_rec(t=39.9))   # not extreme
        acc.add(make_rec(t=0.1))    # not extreme
        acc.add(make_rec(t=-0.1))   # extreme
        snap = acc.snapshot()
        assert snap.extreme_temperature_events == 3

    def test_hottest_measurement(self):
        acc = AnalyticsAccumulator()
        acc.add(make_rec(ts=1000, st=0, t=30.0))
        acc.add(make_rec(ts=2000, st=1, t=45.0))
        acc.add(make_rec(ts=3000, st=2, t=35.0))
        snap = acc.snapshot()
        assert snap.hottest.temperature == 45.0
        assert snap.hottest.station_id == 1

    def test_coldest_measurement(self):
        acc = AnalyticsAccumulator()
        acc.add(make_rec(ts=1000, st=0, t=10.0))
        acc.add(make_rec(ts=2000, st=1, t=-5.0))
        acc.add(make_rec(ts=3000, st=2, t=20.0))
        snap = acc.snapshot()
        assert snap.coldest.temperature == -5.0
        assert snap.coldest.station_id == 1

    def test_hottest_tie_breaking(self):
        acc = AnalyticsAccumulator()
        acc.add(make_rec(ts=2000, st=3, t=45.0))
        acc.add(make_rec(ts=1000, st=5, t=45.0))  # same temp, lower ts → wins
        snap = acc.snapshot()
        assert snap.hottest.timestamp == 1000
        assert snap.hottest.station_id == 5

    def test_busiest_interval(self):
        # bucket = ts // 60
        acc = AnalyticsAccumulator()
        bucket_a = 1600000000 // 60   # e.g. 26666666
        bucket_b = 1600000060 // 60   # e.g. 26666667
        for _ in range(5):
            acc.add(make_rec(ts=1600000000))   # bucket_a
        for _ in range(3):
            acc.add(make_rec(ts=1600000060))   # bucket_b
        snap = acc.snapshot()
        assert snap.busiest_interval == bucket_a
        assert snap.busiest_interval_count == 5

    def test_busiest_interval_tie_lower_wins(self):
        acc = AnalyticsAccumulator()
        bucket_low = 100
        bucket_high = 200
        acc.add(make_rec(ts=bucket_low * 60))
        acc.add(make_rec(ts=bucket_high * 60))
        snap = acc.snapshot()
        # both have count 1 → lower bucket index wins
        assert snap.busiest_interval == bucket_low

    def test_station_stats(self):
        acc = AnalyticsAccumulator(k=3)
        acc.add(make_rec(st=0, t=20.0, r=10.0))
        acc.add(make_rec(st=0, t=30.0, r=20.0))
        acc.add(make_rec(st=1, t=25.0, r=15.0))
        snap = acc.snapshot()
        assert snap.station_stats[0].count == 2
        assert abs(snap.station_stats[0].avg_temperature - 25.0) < 1e-9
        assert abs(snap.station_stats[0].total_rainfall - 30.0) < 1e-9
        assert snap.station_stats[1].count == 1

    def test_top_k_stations_ordering(self):
        acc = AnalyticsAccumulator(k=2)
        for _ in range(5):
            acc.add(make_rec(st=2))
        for _ in range(3):
            acc.add(make_rec(st=0))
        for _ in range(4):
            acc.add(make_rec(st=1))
        snap = acc.snapshot()
        top = snap.top_k_stations()
        assert len(top) == 2
        assert top[0].station_id == 2   # count 5
        assert top[1].station_id == 1   # count 4

    def test_top_k_station_tie_lower_id_wins(self):
        acc = AnalyticsAccumulator(k=2)
        for _ in range(3):
            acc.add(make_rec(st=5))
        for _ in range(3):
            acc.add(make_rec(st=2))
        snap = acc.snapshot()
        top = snap.top_k_stations()
        assert top[0].station_id == 2   # lower id wins on tie

    def test_rainfall_accumulation(self):
        acc = AnalyticsAccumulator()
        acc.add(make_rec(r=10.5))
        acc.add(make_rec(r=20.3))
        acc.add(make_rec(r=5.2))
        snap = acc.snapshot()
        assert abs(snap.total_rainfall - 36.0) < 1e-9
        assert abs(snap.max_rainfall - 20.3) < 1e-9

    def test_wind_stats(self):
        acc = AnalyticsAccumulator()
        acc.add(make_rec(w=10.0))
        acc.add(make_rec(w=50.0))
        acc.add(make_rec(w=30.0))
        snap = acc.snapshot()
        assert abs(snap.avg_wind_speed - (10 + 50 + 30) / 3) < 1e-9
        assert snap.max_wind_speed == 50.0
