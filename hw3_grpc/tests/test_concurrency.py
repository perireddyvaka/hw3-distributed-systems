"""hw3_grpc/tests/test_concurrency.py — Tests for concurrent streaming + querying."""

from __future__ import annotations
import subprocess
import sys
import tempfile
import threading
import time

import grpc
import pytest

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.dataset.generate_dataset import generate_dataset
from hw3_grpc.client.query_client import query
from hw3_grpc.client.streaming_client import stream

CONC_COORD_PORT = 55150
CONC_WORKER_BASE = 55160
STARTUP_WAIT = 3.0


def _start_system(n_workers: int, coord_port: int, worker_base: int, k: int = 5):
    procs = []
    for i in range(n_workers):
        p = subprocess.Popen(
            [sys.executable, "-m", "hw3_grpc.worker.worker_server",
             "--id", str(i), "--port", str(worker_base + i),
             "--k", str(k), "--log-level", "WARNING"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    coord = subprocess.Popen(
        [sys.executable, "-m", "hw3_grpc.coordinator.server",
         "--workers", str(n_workers), "--port", str(coord_port),
         "--worker-base-port", str(worker_base), "--k", str(k),
         "--log-level", "WARNING"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    procs.append(coord)
    time.sleep(STARTUP_WAIT)
    return procs


def _kill_all(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


@pytest.mark.integration
class TestConcurrency:
    """Test that concurrent queries during streaming do not corrupt state."""

    def test_concurrent_queries_during_stream(self, tmp_path):
        """Stream a dataset while multiple query threads poll for analytics."""
        dataset = str(tmp_path / "conc.txt")
        generate_dataset(n=1000, k=5, s=20, filename=dataset, seed=99)

        coord_port = CONC_COORD_PORT
        worker_base = CONC_WORKER_BASE
        procs = _start_system(2, coord_port, worker_base)

        thread_records: dict[int, list[int]] = {i: [] for i in range(3)}
        query_errors = []
        stop_flag = threading.Event()

        def query_thread(tid: int):
            while not stop_flag.is_set():
                try:
                    snap = query(host="localhost", port=coord_port)
                    thread_records[tid].append(snap.total_measurements)
                except Exception as e:
                    query_errors.append(str(e))
                time.sleep(0.02)

        # Start 3 concurrent query threads
        threads = [threading.Thread(target=query_thread, args=(i,), daemon=True) for i in range(3)]
        for t in threads:
            t.start()

        try:
            stats = stream(
                dataset_path=dataset,
                host="localhost",
                port=coord_port,
                batch_size=100,
                delay=0.01,
            )
        finally:
            stop_flag.set()
            for t in threads:
                t.join(timeout=2)
            _kill_all(procs)

        assert stats["success"]
        # Queries should have succeeded without network/server errors
        assert len(query_errors) == 0, f"Query errors: {query_errors[:3]}"

        # Monotonic reads: each thread's observed sequence of counts must be non-decreasing
        for tid, counts in thread_records.items():
            assert len(counts) > 0, f"Thread {tid} observed no queries"
            for i in range(1, len(counts)):
                assert counts[i] >= counts[i - 1], (
                    f"Thread {tid} count regressed: {counts[i-1]} -> {counts[i]}"
                )
