"""hw3_grpc/tests/test_streaming.py — Integration tests for the full streaming pipeline.

These tests start real coordinator + worker processes, stream a small dataset,
then verify the final analytics match a locally-computed reference.
"""

from __future__ import annotations
import subprocess
import sys
import time

import grpc
import pytest

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.common.analytics import AnalyticsAccumulator
from hw3_grpc.common.models import WeatherRecord
from hw3_grpc.dataset.generate_dataset import generate_dataset
from hw3_grpc.client.streaming_client import stream

# Ports chosen to not clash with the default system
TEST_COORDINATOR_PORT = 55050
TEST_WORKER_BASE_PORT = 55060
STARTUP_WAIT = 3.0  # seconds to wait for processes to start


def _start_workers(n: int, base_port: int, k: int = 10):
    procs = []
    for i in range(n):
        port = base_port + i
        p = subprocess.Popen(
            [sys.executable, "-m", "hw3_grpc.worker.worker_server",
             "--id", str(i), "--port", str(port), "--k", str(k), "--log-level", "WARNING"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    return procs


def _start_coordinator(n_workers: int, coord_port: int, worker_base: int, k: int = 10):
    return subprocess.Popen(
        [sys.executable, "-m", "hw3_grpc.coordinator.server",
         "--workers", str(n_workers),
         "--port", str(coord_port),
         "--worker-base-port", str(worker_base),
         "--k", str(k),
         "--log-level", "WARNING"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _kill_all(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def _compute_reference(dataset_path: str, k: int = 10) -> AnalyticsAccumulator:
    """Compute reference analytics using the local AnalyticsAccumulator."""
    acc = AnalyticsAccumulator(k=k)
    with open(dataset_path) as fh:
        n, ki, s = map(int, fh.readline().split())
        for line in fh:
            line = line.strip()
            if line:
                acc.add(WeatherRecord.from_line(line))
    return acc


def _get_analytics(coord_port: int) -> weather_pb2.AnalyticsSnapshot:
    addr = f"localhost:{coord_port}"
    ch = grpc.insecure_channel(addr, options=cfg.GRPC_OPTIONS)
    stub = weather_pb2_grpc.CoordinatorServiceStub(ch)
    snap = stub.GetAnalytics(weather_pb2.AnalyticsRequest(final_only=True), timeout=10)
    ch.close()
    return snap


@pytest.fixture(scope="module")
def small_dataset(tmp_path_factory):
    d = tmp_path_factory.mktemp("data")
    path = str(d / "small.txt")
    generate_dataset(n=500, k=5, s=20, filename=path, seed=42)
    return path


@pytest.mark.integration
class TestStreamingPipeline:
    """End-to-end streaming tests with real gRPC processes."""

    def _run_pipeline(self, dataset_path: str, n_workers: int, k: int = 5):
        coord_port = TEST_COORDINATOR_PORT + n_workers
        worker_base = TEST_WORKER_BASE_PORT + n_workers * 10
        worker_procs = _start_workers(n_workers, worker_base, k=k)
        coord_proc = _start_coordinator(n_workers, coord_port, worker_base, k=k)
        time.sleep(STARTUP_WAIT)
        try:
            stats = stream(
                dataset_path=dataset_path,
                host="localhost",
                port=coord_port,
                batch_size=50,
                delay=0.0,
            )
            assert stats["success"], f"Stream failed: {stats['message']}"
            snap = _get_analytics(coord_port)
            return snap, stats
        finally:
            _kill_all([coord_proc] + worker_procs)

    def test_single_worker(self, small_dataset):
        snap, stats = self._run_pipeline(small_dataset, n_workers=1, k=5)
        assert snap.total_measurements == 500
        assert stats["records_received"] == 500

    def test_two_workers(self, small_dataset):
        snap, stats = self._run_pipeline(small_dataset, n_workers=2, k=5)
        assert snap.total_measurements == 500

    def test_four_workers(self, small_dataset):
        snap, stats = self._run_pipeline(small_dataset, n_workers=4, k=5)
        assert snap.total_measurements == 500

    def test_final_analytics_match_reference(self, small_dataset):
        """HW3 result must match local sequential accumulator for same dataset."""
        snap, _ = self._run_pipeline(small_dataset, n_workers=2, k=5)
        ref_acc = _compute_reference(small_dataset, k=5)
        ref = ref_acc.snapshot()

        assert snap.total_measurements == ref.total_measurements
        assert abs(snap.avg_temperature - ref.avg_temperature) < 1e-4
        assert abs(snap.min_temperature - ref.min_temperature) < 1e-9
        assert abs(snap.max_temperature - ref.max_temperature) < 1e-9
        assert abs(snap.total_rainfall - ref.total_rainfall) < 1e-4
        assert snap.extreme_temperature_events == ref.extreme_temperature_events
        assert snap.hottest.temperature == ref.hottest.temperature
        assert snap.coldest.temperature == ref.coldest.temperature
        assert snap.busiest_interval == ref.busiest_interval
