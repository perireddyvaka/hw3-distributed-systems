"""hw3_grpc/coordinator/state.py — Thread-safe global analytics state."""

from __future__ import annotations
import threading

from hw3_grpc.common.models import AnalyticsSnapshot


class GlobalAnalyticsState:
    """Thread-safe global analytics state maintained by the coordinator.

    The coordinator periodically updates this state by merging worker snapshots.
    Query clients read from this state concurrently.

    Consistency model:
        Queries return a consistent snapshot of the state at some recent point.
        The final snapshot after stream completion is exact.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._snapshot: AnalyticsSnapshot = AnalyticsSnapshot()
        self._stream_done: bool = False

    def update(self, snapshot: AnalyticsSnapshot) -> None:
        """Replace the current global snapshot (monotonically non-decreasing)."""
        with self._lock:
            if snapshot.total_measurements >= self._snapshot.total_measurements:
                self._snapshot = snapshot

    def get_snapshot(self) -> AnalyticsSnapshot:
        """Return a reference to the latest snapshot (thread-safe)."""
        with self._lock:
            return self._snapshot

    def mark_stream_done(self) -> None:
        with self._lock:
            self._stream_done = True

    @property
    def is_stream_done(self) -> bool:
        with self._lock:
            return self._stream_done
