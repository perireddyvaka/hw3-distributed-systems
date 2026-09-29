"""hw3_grpc/worker/worker_state.py — Worker-local analytics state."""

from __future__ import annotations
import threading

from hw3_grpc.common.analytics import AnalyticsAccumulator
from hw3_grpc.common.models import AnalyticsSnapshot, WeatherRecord


class WorkerLocalState:
    """Thread-safe container for a single worker's local analytics state.

    The worker accumulates records incrementally. The coordinator calls
    get_snapshot() to retrieve the current state for global aggregation.
    """

    def __init__(self, worker_id: int, k: int = 10) -> None:
        self.worker_id = worker_id
        self._k = k
        self._lock = threading.Lock()
        self._acc = AnalyticsAccumulator(k=k)

    def process_records(self, records: list[WeatherRecord]) -> None:
        """Process a list of records and update local state (thread-safe)."""
        with self._lock:
            for rec in records:
                self._acc.add(rec)

    def get_snapshot(self) -> AnalyticsSnapshot:
        """Return a consistent snapshot of the current local state (thread-safe)."""
        with self._lock:
            return self._acc.snapshot()

    @property
    def count(self) -> int:
        with self._lock:
            return self._acc.count

    def reset(self) -> None:
        """Reset local analytics accumulator to initial empty state."""
        with self._lock:
            self._acc = AnalyticsAccumulator(k=self._k)
