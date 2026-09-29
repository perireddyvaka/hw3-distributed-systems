"""hw3_grpc/tests/test_correctness.py — End-to-end correctness: HW3 vs HW2 oracle.

For each test case:
1. Generate a reproducible dataset using the HW2-compatible generator.
2. Run the HW2 sequential binary (q8_seq) on it → reference output.
3. Run the HW3 gRPC pipeline on the same dataset → HW3 output.
4. Compare every metric field-by-field.

The HW2 sequential binary must be compiled first:
    g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
"""

from __future__ import annotations
import os
import subprocess
import sys
import time
from typing import Dict, Any

import pytest

try:
    from hw3_grpc.generated import weather_pb2, weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

import grpc
from hw3_grpc.common import config as cfg
from hw3_grpc.client.streaming_client import stream
from hw3_grpc.dataset.generate_dataset import generate_dataset

CORR_COORD_PORT = 55200
CORR_WORKER_BASE = 55210
HW2_SEQ_BIN = "hw2_mpi/src/q8_seq"
HW2_SEQ_SRC = "hw2_mpi/src/q8_seq.cpp"
STARTUP_WAIT = 3.0
FLOAT_TOL = 1e-4   # tolerance for floating-point comparisons


# ── HW2 reference oracle ──────────────────────────────────────────────────────

def _compile_hw2_seq():
    """Compile HW2 sequential binary if not already compiled."""
    if not os.path.exists(HW2_SEQ_BIN):
        result = subprocess.run(
            ["g++", "-O2", "-std=c++17", "-o", HW2_SEQ_BIN, HW2_SEQ_SRC],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to compile HW2 seq:\n{result.stderr}")


def _parse_hw2_output(text: str, k: int) -> Dict[str, Any]:
    """Parse HW2 sequential text output into a comparable dict."""
    out: Dict[str, Any] = {}
    lines = text.strip().splitlines()
    top_stations = []
    in_top = False
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line == "TOP_STATIONS":
            in_top = True
            continue
        if in_top:
            parts = line.split()
            top_stations.append({
                "station_id": int(parts[0]),
                "count": int(parts[1]),
                "avg_temperature": float(parts[2]),
                "total_rainfall": float(parts[3]),
            })
            continue
        parts = line.split(None, 1)
        key, val = parts[0], parts[1] if len(parts) > 1 else ""
        if key in ("TOTAL_MEASUREMENTS", "EXTREME_TEMPERATURE_EVENTS"):
            out[key] = int(val)
        elif key == "HOTTEST_MEASUREMENT":
            p = val.split()
            out[key] = {"timestamp": int(p[0]), "station_id": int(p[1]), "temperature": float(p[2])}
        elif key == "COLDEST_MEASUREMENT":
            p = val.split()
            out[key] = {"timestamp": int(p[0]), "station_id": int(p[1]), "temperature": float(p[2])}
        elif key == "BUSIEST_INTERVAL":
            p = val.split()
            out[key] = {"interval": int(p[0]), "count": int(p[1])}
        else:
            out[key] = float(val)
    out["TOP_STATIONS"] = top_stations
    return out


def _run_hw2_seq(dataset_path: str, k: int) -> Dict[str, Any]:
    """Run the HW2 sequential oracle and parse its output into a dict."""
    _compile_hw2_seq()
    result = subprocess.run(
        [HW2_SEQ_BIN, dataset_path],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"HW2 seq failed:\n{result.stderr}")
    return _parse_hw2_output(result.stdout, k)


# ── HW3 system helpers ────────────────────────────────────────────────────────

def _start_system(n_workers: int, coord_port: int, worker_base: int, k: int):
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


def _get_hw3_analytics(coord_port: int, k: int) -> Dict[str, Any]:
    """Query coordinator and convert to comparable dict."""
    addr = f"localhost:{coord_port}"
    ch = grpc.insecure_channel(addr, options=cfg.GRPC_OPTIONS)
    stub = weather_pb2_grpc.CoordinatorServiceStub(ch)
    snap = stub.GetAnalytics(weather_pb2.AnalyticsRequest(final_only=True), timeout=10)
    ch.close()

    out: Dict[str, Any] = {
        "TOTAL_MEASUREMENTS": snap.total_measurements,
        "AVERAGE_TEMPERATURE": snap.avg_temperature,
        "MIN_TEMPERATURE": snap.min_temperature,
        "MAX_TEMPERATURE": snap.max_temperature,
        "AVERAGE_HUMIDITY": snap.avg_humidity,
        "MIN_HUMIDITY": snap.min_humidity,
        "MAX_HUMIDITY": snap.max_humidity,
        "AVERAGE_PRESSURE": snap.avg_pressure,
        "MIN_PRESSURE": snap.min_pressure,
        "MAX_PRESSURE": snap.max_pressure,
        "TOTAL_RAINFALL": snap.total_rainfall,
        "MAX_RAINFALL": snap.max_rainfall,
        "AVERAGE_WIND_SPEED": snap.avg_wind_speed,
        "MAX_WIND_SPEED": snap.max_wind_speed,
        "EXTREME_TEMPERATURE_EVENTS": snap.extreme_temperature_events,
        "HOTTEST_MEASUREMENT": {
            "timestamp": snap.hottest.timestamp,
            "station_id": snap.hottest.station_id,
            "temperature": snap.hottest.temperature,
        },
        "COLDEST_MEASUREMENT": {
            "timestamp": snap.coldest.timestamp,
            "station_id": snap.coldest.station_id,
            "temperature": snap.coldest.temperature,
        },
        "BUSIEST_INTERVAL": {
            "interval": snap.busiest_interval,
            "count": snap.busiest_interval_count,
        },
        "TOP_STATIONS": [
            {
                "station_id": s.station_id,
                "count": s.count,
                "avg_temperature": s.avg_temperature,
                "total_rainfall": s.total_rainfall,
            }
            for s in snap.top_stations
        ],
    }
    return out


def _assert_results_match(hw2: Dict, hw3: Dict, tol: float = FLOAT_TOL):
    """Compare HW2 and HW3 result dicts field by field."""
    # Exact integer fields
    for key in ("TOTAL_MEASUREMENTS", "EXTREME_TEMPERATURE_EVENTS"):
        assert hw2[key] == hw3[key], f"{key}: HW2={hw2[key]} HW3={hw3[key]}"

    # Float fields (with tolerance)
    float_keys = [
        "AVERAGE_TEMPERATURE", "MIN_TEMPERATURE", "MAX_TEMPERATURE",
        "AVERAGE_HUMIDITY", "MIN_HUMIDITY", "MAX_HUMIDITY",
        "AVERAGE_PRESSURE", "MIN_PRESSURE", "MAX_PRESSURE",
        "TOTAL_RAINFALL", "MAX_RAINFALL",
        "AVERAGE_WIND_SPEED", "MAX_WIND_SPEED",
    ]
    for key in float_keys:
        assert abs(hw2[key] - hw3[key]) < tol, f"{key}: HW2={hw2[key]:.8f} HW3={hw3[key]:.8f}"

    # Hottest
    for sub in ("timestamp", "station_id"):
        assert hw2["HOTTEST_MEASUREMENT"][sub] == hw3["HOTTEST_MEASUREMENT"][sub], \
            f"HOTTEST {sub}: HW2={hw2['HOTTEST_MEASUREMENT'][sub]} HW3={hw3['HOTTEST_MEASUREMENT'][sub]}"
    assert abs(hw2["HOTTEST_MEASUREMENT"]["temperature"] - hw3["HOTTEST_MEASUREMENT"]["temperature"]) < tol

    # Coldest
    for sub in ("timestamp", "station_id"):
        assert hw2["COLDEST_MEASUREMENT"][sub] == hw3["COLDEST_MEASUREMENT"][sub], \
            f"COLDEST {sub}: HW2={hw2['COLDEST_MEASUREMENT'][sub]} HW3={hw3['COLDEST_MEASUREMENT'][sub]}"

    # Busiest interval
    assert hw2["BUSIEST_INTERVAL"]["interval"] == hw3["BUSIEST_INTERVAL"]["interval"], \
        f"BUSIEST_INTERVAL: HW2={hw2['BUSIEST_INTERVAL']} HW3={hw3['BUSIEST_INTERVAL']}"
    assert hw2["BUSIEST_INTERVAL"]["count"] == hw3["BUSIEST_INTERVAL"]["count"]

    # Top stations (order and values)
    assert len(hw2["TOP_STATIONS"]) == len(hw3["TOP_STATIONS"]), \
        f"TOP_STATIONS length: HW2={len(hw2['TOP_STATIONS'])} HW3={len(hw3['TOP_STATIONS'])}"
    for i, (ref_s, hw3_s) in enumerate(zip(hw2["TOP_STATIONS"], hw3["TOP_STATIONS"])):
        assert ref_s["station_id"] == hw3_s["station_id"], \
            f"TOP_STATIONS[{i}] station_id: HW2={ref_s['station_id']} HW3={hw3_s['station_id']}"
        assert ref_s["count"] == hw3_s["count"], \
            f"TOP_STATIONS[{i}] count: HW2={ref_s['count']} HW3={hw3_s['count']}"
        assert abs(ref_s["avg_temperature"] - hw3_s["avg_temperature"]) < tol
        assert abs(ref_s["total_rainfall"] - hw3_s["total_rainfall"]) < tol


# ── Test Cases ────────────────────────────────────────────────────────────────

TEST_CASES = [
    {"n": 1000, "k": 5, "s": 10, "seed": 42, "label": "small_1k"},
    {"n": 5000, "k": 10, "s": 50, "seed": 99, "label": "medium_5k"},
    {"n": 10000, "k": 20, "s": 100, "seed": 123, "label": "large_10k"},
    {"n": 99999, "k": 15, "s": 250, "seed": 111, "label": "prime_99999"},  # prime N
]

WORKER_COUNTS = [1, 2, 4]


@pytest.mark.integration
@pytest.mark.correctness
class TestCorrectnessVsHW2:
    """Compare HW3 gRPC results against the HW2 sequential oracle."""

    @pytest.mark.parametrize("case", TEST_CASES, ids=[c["label"] for c in TEST_CASES])
    @pytest.mark.parametrize("n_workers", WORKER_COUNTS)
    def test_hw3_matches_hw2_seq(self, case, n_workers, tmp_path):
        n, k, s, seed = case["n"], case["k"], case["s"], case["seed"]
        dataset_path = str(tmp_path / f"{case['label']}.txt")
        generate_dataset(n=n, k=k, s=s, filename=dataset_path, seed=seed)

        # HW2 reference
        hw2_result = _run_hw2_seq(dataset_path, k)

        # HW3 pipeline — use unique ports per (case × workers) combination
        port_offset = TEST_CASES.index(case) * 10 + WORKER_COUNTS.index(n_workers)
        coord_port = CORR_COORD_PORT + port_offset
        worker_base = CORR_WORKER_BASE + port_offset * 20

        procs = _start_system(n_workers, coord_port, worker_base, k)
        try:
            stats = stream(
                dataset_path=dataset_path,
                host="localhost",
                port=coord_port,
                batch_size=100,
                delay=0.0,
            )
            assert stats["success"]
            hw3_result = _get_hw3_analytics(coord_port, k)
        finally:
            _kill_all(procs)

        _assert_results_match(hw2_result, hw3_result)
