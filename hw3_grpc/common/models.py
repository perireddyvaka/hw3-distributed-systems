"""hw3_grpc/common/models.py — Python data models for HW3 Weather Analytics."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class WeatherRecord:
    """A single weather sensor measurement.

    Matches the HW2 dataset format exactly:
        timestamp  station_id  temperature  humidity  pressure  rainfall  wind_speed
    """
    timestamp: int
    station_id: int
    temperature: float
    humidity: float
    pressure: float
    rainfall: float
    wind_speed: float

    @classmethod
    def from_line(cls, line: str) -> "WeatherRecord":
        """Parse a space-separated data line (not the header line)."""
        parts = line.strip().split()
        if len(parts) != 7:
            raise ValueError(f"Expected 7 fields, got {len(parts)}: {line!r}")
        return cls(
            timestamp=int(parts[0]),
            station_id=int(parts[1]),
            temperature=float(parts[2]),
            humidity=float(parts[3]),
            pressure=float(parts[4]),
            rainfall=float(parts[5]),
            wind_speed=float(parts[6]),
        )


@dataclass
class MeasurementRef:
    """Identifies a single record (used for hottest/coldest tracking)."""
    timestamp: int = 0
    station_id: int = 0
    temperature: float = 0.0


@dataclass
class StationStat:
    """Per-station accumulated statistics."""
    station_id: int
    count: int = 0
    sum_temperature: float = 0.0
    sum_rainfall: float = 0.0

    @property
    def avg_temperature(self) -> float:
        return self.sum_temperature / self.count if self.count > 0 else 0.0

    @property
    def total_rainfall(self) -> float:
        return self.sum_rainfall


@dataclass
class AnalyticsSnapshot:
    """Complete analytics state — mirrors the HW2 sequential output format."""
    total_measurements: int = 0
    # temperature
    sum_temperature: float = 0.0
    min_temperature: float = 1e18
    max_temperature: float = -1e18
    # humidity
    sum_humidity: float = 0.0
    min_humidity: float = 1e18
    max_humidity: float = -1e18
    # pressure
    sum_pressure: float = 0.0
    min_pressure: float = 1e18
    max_pressure: float = -1e18
    # rainfall / wind
    total_rainfall: float = 0.0
    max_rainfall: float = -1e18
    sum_wind_speed: float = 0.0
    max_wind_speed: float = -1e18
    # extremes
    extreme_temperature_events: int = 0
    hottest: MeasurementRef = field(default_factory=lambda: MeasurementRef(0, 0, -1e18))
    coldest: MeasurementRef = field(default_factory=lambda: MeasurementRef(0, 0, 1e18))
    # interval
    interval_counts: Dict[int, int] = field(default_factory=dict)
    # stations
    station_stats: Dict[int, StationStat] = field(default_factory=dict)
    # top-K parameter
    k: int = 10

    # ── derived properties ──────────────────────────────────────────────────────

    @property
    def avg_temperature(self) -> float:
        n = self.total_measurements
        return self.sum_temperature / n if n > 0 else 0.0

    @property
    def avg_humidity(self) -> float:
        n = self.total_measurements
        return self.sum_humidity / n if n > 0 else 0.0

    @property
    def avg_pressure(self) -> float:
        n = self.total_measurements
        return self.sum_pressure / n if n > 0 else 0.0

    @property
    def avg_wind_speed(self) -> float:
        n = self.total_measurements
        return self.sum_wind_speed / n if n > 0 else 0.0

    @property
    def busiest_interval(self) -> Optional[int]:
        """Return the interval bucket with the highest count (tie → lower index)."""
        if not self.interval_counts:
            return None
        best_interval = -1
        best_count = -1
        for interval, count in self.interval_counts.items():
            if count > best_count or (count == best_count and interval < best_interval):
                best_count = count
                best_interval = interval
        return best_interval

    @property
    def busiest_interval_count(self) -> int:
        bi = self.busiest_interval
        if bi is None:
            return 0
        return self.interval_counts.get(bi, 0)

    def top_k_stations(self) -> list:
        """Return up to K stations sorted by (count desc, station_id asc)."""
        stations = list(self.station_stats.values())
        stations.sort(key=lambda s: (-s.count, s.station_id))
        return stations[: self.k]
