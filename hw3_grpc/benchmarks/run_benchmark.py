"""
hw3_grpc/benchmarks/run_benchmark.py — Comprehensive HW3 performance benchmark runner.

Runs controlled performance experiments for the HW3 gRPC analytics system:
  1. Worker scaling:
         Fix N=100K, batch=500; vary NUM_WORKERS in [1, 2, 4, 8].
         Measures: throughput (rec/s), total time, speedup.
  2. Message batch granularity:
         Fix N=100K, workers=4; vary BATCH_SIZE in [10, 50, 100, 500, 1000, 5000].
         Measures: ingestion time, streaming throughput.
  3. Query frequency / concurrency:
         Fix N=100K, batch=500, workers=4; vary concurrent query clients in [0, 1, 2, 4, 8].
         Measures: query latency (p50, p95, p99, avg), query throughput, ingestion throughput impact.
  4. Dataset size scaling:
         Fix workers=4, batch=500; vary N in [10K, 50K, 100K, 250K, 500K].
         Compares against HW2 sequential oracle (q8_seq).

Output:
  - CSV files in:   hw3_grpc/benchmarks/results/
  - Plot images in: hw3_grpc/benchmarks/plots/

Usage:
  python hw3_grpc/benchmarks/run_benchmark.py --all
  python hw3_grpc/benchmarks/run_benchmark.py --experiment scaling
  python hw3_grpc/benchmarks/run_benchmark.py --experiment batch
  python hw3_grpc/benchmarks/run_benchmark.py --experiment query
  python hw3_grpc/benchmarks/run_benchmark.py --experiment size
"""

from __future__ import annotations
import argparse
import csv
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Optional

import numpy as np

# Set matplotlib backend to Agg for headless environments
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from hw3_grpc.client.query_client import query
from hw3_grpc.client.streaming_client import stream
from hw3_grpc.dataset.generate_dataset import generate_dataset

RESULTS_DIR = REPO_ROOT / "hw3_grpc" / "benchmarks" / "results"
PLOTS_DIR = REPO_ROOT / "hw3_grpc" / "benchmarks" / "plots"
HW2_SEQ_BIN = REPO_ROOT / "hw2_mpi" / "src" / "q8_seq"
HW2_SEQ_SRC = REPO_ROOT / "hw2_mpi" / "src" / "q8_seq.cpp"

BASE_COORD_PORT = 56000
BASE_WORKER_PORT = 56100
STARTUP_WAIT = 2.5


# ── Process Management Helpers ─────────────────────────────────────────────────

def start_cluster(n_workers: int, coord_port: int, worker_base: int, k: int = 10) -> List[subprocess.Popen]:
    """Launch N workers and 1 coordinator as background subprocesses."""
    procs: List[subprocess.Popen] = []
    for i in range(n_workers):
        p = subprocess.Popen(
            [
                sys.executable, "-m", "hw3_grpc.worker.worker_server",
                "--id", str(i),
                "--port", str(worker_base + i),
                "--k", str(k),
                "--log-level", "WARNING",
            ],
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(p)

    coord = subprocess.Popen(
        [
            sys.executable, "-m", "hw3_grpc.coordinator.server",
            "--workers", str(n_workers),
            "--port", str(coord_port),
            "--worker-base-port", str(worker_base),
            "--k", str(k),
            "--log-level", "WARNING",
        ],
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    procs.append(coord)
    time.sleep(STARTUP_WAIT)
    return procs


def stop_cluster(procs: List[subprocess.Popen]) -> None:
    """Terminate all cluster subprocesses gracefully."""
    for p in procs:
        try:
            p.terminate()
        except OSError:
            pass
    for p in procs:
        try:
            p.wait(timeout=4)
        except (subprocess.TimeoutExpired, OSError):
            p.kill()


class ResourceMonitor:
    """Monitors peak RSS memory usage across a cluster of processes."""

    def __init__(self, pids: List[int], interval: float = 0.05):
        self.pids = pids
        self.interval = interval
        self.peak_rss_mb = 0.0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _sample(self) -> float:
        total_mb = 0.0
        for pid in self.pids:
            try:
                with open(f"/proc/{pid}/statm", "r") as f:
                    pages = int(f.read().split()[1])
                    total_mb += (pages * 4096) / (1024 * 1024)
            except (OSError, IndexError, ValueError):
                continue
        return total_mb

    def _run(self) -> None:
        while not self._stop_event.is_set():
            cur = self._sample()
            if cur > self.peak_rss_mb:
                self.peak_rss_mb = cur
            self._stop_event.wait(self.interval)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> float:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        final_sample = self._sample()
        if final_sample > self.peak_rss_mb:
            self.peak_rss_mb = final_sample
        return round(self.peak_rss_mb, 2)


def ensure_hw2_seq_compiled() -> None:
    """Compile HW2 sequential oracle if not present."""
    if not HW2_SEQ_BIN.exists():
        print("[benchmark] Compiling HW2 sequential binary...")
        subprocess.run(
            ["g++", "-O2", "-std=c++17", "-o", str(HW2_SEQ_BIN), str(HW2_SEQ_SRC)],
            check=True,
        )


# ── Plot Styling ───────────────────────────────────────────────────────────────

def setup_plot_style():
    """Apply clean, modern styling to matplotlib plots."""
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.labelsize": 11,
        "axes.labelweight": "semibold",
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "figure.titlesize": 14,
        "figure.dpi": 200,
        "grid.color": "#e0e0e0",
        "grid.linestyle": "--",
        "grid.alpha": 0.7,
    })


# ── Benchmark 1: Worker Scaling ───────────────────────────────────────────────

def run_worker_scaling(n_records: int = 100_000, batch_size: int = 500, workers_list: Optional[List[int]] = None):
    """Vary NUM_WORKERS and measure ingestion throughput and speedup."""
    if workers_list is None:
        workers_list = [1, 2, 4, 8]

    print("\n" + "=" * 65)
    print(f"  EXPERIMENT 1: Worker Scaling (N={n_records:,}, Batch={batch_size})")
    print("=" * 65)

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_path = os.path.join(tmpdir, "scaling.txt")
        print(f"[benchmark] Generating dataset with {n_records:,} records...")
        generate_dataset(n=n_records, k=10, s=50, filename=dataset_path, seed=42)

        results = []
        base_time = None

        for w in workers_list:
            port = BASE_COORD_PORT + w * 10
            worker_base = BASE_WORKER_PORT + w * 10
            print(f"[benchmark] Testing with {w} worker(s)...")

            procs = start_cluster(w, port, worker_base, k=10)
            monitor = ResourceMonitor([p.pid for p in procs])
            monitor.start()
            try:
                t0 = time.perf_counter()
                stats = stream(
                    dataset_path=dataset_path,
                    host="localhost",
                    port=port,
                    batch_size=batch_size,
                    delay=0.0,
                )
                t_total = time.perf_counter() - t0
                peak_mem = monitor.stop()

                if not stats["success"]:
                    raise RuntimeError(f"Streaming failed for {w} workers: {stats.get('error')}")

                throughput = n_records / t_total if t_total > 0 else 0
                if base_time is None:
                    base_time = t_total
                speedup = base_time / t_total if t_total > 0 else 1.0

                results.append({
                    "workers": w,
                    "records": n_records,
                    "batch_size": batch_size,
                    "total_time_sec": round(t_total, 4),
                    "throughput_rec_per_sec": round(throughput, 1),
                    "speedup": round(speedup, 3),
                    "peak_memory_mb": peak_mem,
                })
                print(
                    f"  -> Workers: {w:2d} | Time: {t_total:6.3f}s | "
                    f"Throughput: {throughput:9.1f} rec/s | Speedup: {speedup:5.2f}x | "
                    f"Peak RAM: {peak_mem:.1f} MB"
                )
            finally:
                monitor.stop()
                stop_cluster(procs)
                time.sleep(0.5)

    # Save CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_file = RESULTS_DIR / "worker_scaling.csv"
    with open(csv_file, "w", newline="") as f:
        fields = [
            "workers", "records", "batch_size", "total_time_sec",
            "throughput_rec_per_sec", "speedup", "peak_memory_mb"
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(f"[benchmark] Saved CSV results: {csv_file}")

    # Plot
    setup_plot_style()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    ws = [r["workers"] for r in results]
    tps = [r["throughput_rec_per_sec"] for r in results]
    speedups = [r["speedup"] for r in results]

    # Subplot 1: Throughput
    ax1.plot(ws, tps, "o-", color="#1f77b4", linewidth=2.2, markersize=7, label="gRPC Pipeline")
    ax1.set_xlabel("Number of Workers")
    ax1.set_ylabel("Throughput (records/sec)")
    ax1.set_title("Ingestion Throughput vs Worker Count")
    ax1.set_xticks(ws)
    ax1.legend(loc="upper left")
    for x, y in zip(ws, tps):
        ax1.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)

    # Subplot 2: Speedup
    ax2.plot(ws, speedups, "s-", color="#2ca02c", linewidth=2.2, markersize=7, label="Observed Speedup")
    ax2.plot(ws, ws, "--", color="#7f7f7f", linewidth=1.5, label="Ideal Linear Speedup")
    ax2.set_xlabel("Number of Workers")
    ax2.set_ylabel("Speedup relative to 1 Worker")
    ax2.set_title("Scaling Speedup vs Worker Count")
    ax2.set_xticks(ws)
    ax2.legend(loc="upper left")
    for x, y in zip(ws, speedups):
        ax2.annotate(f"{y:.2f}x", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)

    fig.tight_layout()
    plot_file = PLOTS_DIR / "worker_scaling.png"
    fig.savefig(plot_file, dpi=200)
    plt.close(fig)
    print(f"[benchmark] Saved plot: {plot_file}")
    return results


# ── Benchmark 2: Batch Granularity ─────────────────────────────────────────────

def run_batch_granularity(n_records: int = 100_000, n_workers: int = 4, batch_sizes: Optional[List[int]] = None):
    """Vary BATCH_SIZE and measure streaming ingestion throughput."""
    if batch_sizes is None:
        batch_sizes = [10, 50, 100, 500, 1000, 5000]

    print("\n" + "=" * 65)
    print(f"  EXPERIMENT 2: Message Batch Granularity (N={n_records:,}, Workers={n_workers})")
    print("=" * 65)

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_path = os.path.join(tmpdir, "batch.txt")
        print(f"[benchmark] Generating dataset with {n_records:,} records...")
        generate_dataset(n=n_records, k=10, s=50, filename=dataset_path, seed=43)

        results = []
        port = BASE_COORD_PORT + 120
        worker_base = BASE_WORKER_PORT + 120

        procs = start_cluster(n_workers, port, worker_base, k=10)
        try:
            for b in batch_sizes:
                print(f"[benchmark] Testing batch size: {b:5d}...")
                t0 = time.perf_counter()
                stats = stream(
                    dataset_path=dataset_path,
                    host="localhost",
                    port=port,
                    batch_size=b,
                    delay=0.0,
                )
                t_total = time.perf_counter() - t0

                if not stats["success"]:
                    raise RuntimeError(f"Streaming failed for batch {b}: {stats.get('error')}")

                throughput = n_records / t_total if t_total > 0 else 0
                results.append({
                    "batch_size": b,
                    "records": n_records,
                    "workers": n_workers,
                    "total_time_sec": round(t_total, 4),
                    "throughput_rec_per_sec": round(throughput, 1),
                })
                print(f"  -> Batch: {b:5d} | Time: {t_total:6.3f}s | Throughput: {throughput:9.1f} rec/s")
        finally:
            stop_cluster(procs)
            time.sleep(0.5)

    # Save CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_file = RESULTS_DIR / "batch_granularity.csv"
    with open(csv_file, "w", newline="") as f:
        fields = ["batch_size", "records", "workers", "total_time_sec", "throughput_rec_per_sec"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(f"[benchmark] Saved CSV results: {csv_file}")

    # Plot
    setup_plot_style()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 5))

    bs = [r["batch_size"] for r in results]
    tps = [r["throughput_rec_per_sec"] for r in results]

    ax.plot(bs, tps, "o-", color="#d62728", linewidth=2.2, markersize=7)
    ax.set_xscale("log")
    ax.set_xlabel("Batch Size (records per gRPC message)")
    ax.set_ylabel("Streaming Throughput (records/sec)")
    ax.set_title(f"Throughput vs Message Batch Size (N={n_records:,}, Workers={n_workers})")
    ax.set_xticks(bs)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

    for x, y in zip(bs, tps):
        ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)

    fig.tight_layout()
    plot_file = PLOTS_DIR / "batch_granularity.png"
    fig.savefig(plot_file, dpi=200)
    plt.close(fig)
    print(f"[benchmark] Saved plot: {plot_file}")
    return results


# ── Benchmark 3: Query Concurrency & Latency ──────────────────────────────────

def run_query_concurrency(
    n_records: int = 100_000,
    batch_size: int = 500,
    n_workers: int = 4,
    client_counts: Optional[List[int]] = None,
):
    """Vary concurrent query clients during active streaming and measure latency."""
    if client_counts is None:
        client_counts = [0, 1, 2, 4, 8]

    print("\n" + "=" * 65)
    print(f"  EXPERIMENT 3: Query Concurrency & Latency (N={n_records:,}, Workers={n_workers})")
    print("=" * 65)

    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_path = os.path.join(tmpdir, "query.txt")
        print(f"[benchmark] Generating dataset with {n_records:,} records...")
        generate_dataset(n=n_records, k=10, s=50, filename=dataset_path, seed=44)

        results = []
        for q_clients in client_counts:
            port = BASE_COORD_PORT + 200 + q_clients * 5
            worker_base = BASE_WORKER_PORT + 200 + q_clients * 5
            print(f"[benchmark] Testing with {q_clients} concurrent query client(s)...")

            procs = start_cluster(n_workers, port, worker_base, k=10)
            latencies_ms: List[float] = []
            stop_queries = threading.Event()

            def query_worker():
                while not stop_queries.is_set():
                    try:
                        t_start = time.perf_counter()
                        _ = query(host="localhost", port=port)
                        t_latency = (time.perf_counter() - t_start) * 1000.0  # ms
                        latencies_ms.append(t_latency)
                    except Exception:
                        pass
                    time.sleep(0.015)  # ~66 queries/sec per client

            threads = [threading.Thread(target=query_worker, daemon=True) for _ in range(q_clients)]
            for t in threads:
                t.start()

            try:
                t0 = time.perf_counter()
                stats = stream(
                    dataset_path=dataset_path,
                    host="localhost",
                    port=port,
                    batch_size=batch_size,
                    delay=0.0005,  # slight delay to give queries ample time to run concurrently
                )
                t_stream = time.perf_counter() - t0
                if not stats.get("success", False):
                    print(f"[warning] stream reported failure: {stats.get('error')}")
            finally:
                stop_queries.set()
                for t in threads:
                    t.join(timeout=2)
                stop_cluster(procs)
                time.sleep(0.5)

            stream_tp = n_records / t_stream if t_stream > 0 else 0
            if latencies_ms:
                p50 = float(np.percentile(latencies_ms, 50))
                p95 = float(np.percentile(latencies_ms, 95))
                p99 = float(np.percentile(latencies_ms, 99))
                avg_lat = float(np.mean(latencies_ms))
            else:
                p50 = p95 = p99 = avg_lat = 0.0

            results.append({
                "concurrent_clients": q_clients,
                "total_queries": len(latencies_ms),
                "stream_throughput": round(stream_tp, 1),
                "p50_latency_ms": round(p50, 2),
                "p95_latency_ms": round(p95, 2),
                "p99_latency_ms": round(p99, 2),
                "avg_latency_ms": round(avg_lat, 2),
            })
            print(
                f"  -> Clients: {q_clients:2d} | Queries: {len(latencies_ms):4d} | "
                f"Latency p50: {p50:5.2f}ms | p95: {p95:5.2f}ms | Stream TP: {stream_tp:7.1f} rec/s"
            )

    # Save CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_file = RESULTS_DIR / "query_concurrency.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "concurrent_clients", "total_queries", "stream_throughput",
            "p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "avg_latency_ms"
        ])
        writer.writeheader()
        writer.writerows(results)
    print(f"[benchmark] Saved CSV results: {csv_file}")

    # Plot
    setup_plot_style()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    clients = [r["concurrent_clients"] for r in results]
    p50s = [r["p50_latency_ms"] for r in results]
    p95s = [r["p95_latency_ms"] for r in results]
    stream_tps = [r["stream_throughput"] for r in results]

    # Subplot 1: Latency percentiles
    clients_active = [c for c in clients if c > 0]
    p50_active = [p50s[i] for i, c in enumerate(clients) if c > 0]
    p95_active = [p95s[i] for i, c in enumerate(clients) if c > 0]

    if clients_active:
        ax1.plot(clients_active, p50_active, "o-", color="#2ca02c", linewidth=2, label="p50 (Median)")
        ax1.plot(clients_active, p95_active, "^-", color="#d62728", linewidth=2, label="p95 (Tail)")
        ax1.set_xlabel("Concurrent Query Clients")
        ax1.set_ylabel("Query Latency (ms)")
        ax1.set_title("Query Latency vs Concurrent Clients")
        ax1.set_xticks(clients_active)
        ax1.legend(loc="upper left")

    # Subplot 2: Ingestion Throughput impact
    ax2.bar([str(c) for c in clients], stream_tps, color="#1f77b4", alpha=0.85, width=0.5)
    ax2.set_xlabel("Concurrent Query Clients")
    ax2.set_ylabel("Streaming Ingestion Throughput (rec/s)")
    ax2.set_title("Ingestion Throughput under Query Load")

    fig.tight_layout()
    plot_file = PLOTS_DIR / "query_latency.png"
    fig.savefig(plot_file, dpi=200)
    plt.close(fig)
    print(f"[benchmark] Saved plot: {plot_file}")
    return results


# ── Benchmark 4: Dataset Size Scaling (HW3 vs HW2 Sequential) ─────────────────

def run_dataset_scaling(dataset_sizes: Optional[List[int]] = None, n_workers: int = 4, batch_size: int = 500):
    """Vary dataset size and compare HW3 gRPC pipeline against HW2 sequential oracle."""
    ensure_hw2_seq_compiled()

    if dataset_sizes is None:
        dataset_sizes = [10_000, 50_000, 100_000, 250_000, 500_000]

    print("\n" + "=" * 65)
    print(f"  EXPERIMENT 4: Dataset Size Scaling (Workers={n_workers}, Batch={batch_size})")
    print("=" * 65)

    results = []
    port = BASE_COORD_PORT + 350
    worker_base = BASE_WORKER_PORT + 350

    with tempfile.TemporaryDirectory() as tmpdir:
        for n in dataset_sizes:
            dataset_path = os.path.join(tmpdir, f"data_{n}.txt")
            print(f"[benchmark] Generating dataset with {n:,} records...")
            generate_dataset(n=n, k=10, s=50, filename=dataset_path, seed=45)

            # 1. Run HW2 sequential baseline
            t_seq_start = time.perf_counter()
            seq_res = subprocess.run([str(HW2_SEQ_BIN), dataset_path], capture_output=True, text=True)
            t_seq = time.perf_counter() - t_seq_start
            if seq_res.returncode != 0:
                print(f"[warning] HW2 seq run failed for N={n}: {seq_res.stderr}")
                t_seq = 0.0

            # 2. Run HW3 gRPC system
            procs = start_cluster(n_workers, port, worker_base, k=10)
            try:
                t_hw3_start = time.perf_counter()
                stats = stream(
                    dataset_path=dataset_path,
                    host="localhost",
                    port=port,
                    batch_size=batch_size,
                    delay=0.0,
                )
                t_hw3 = time.perf_counter() - t_hw3_start
                if not stats["success"]:
                    raise RuntimeError(f"HW3 streaming failed for N={n}: {stats.get('error')}")
            finally:
                stop_cluster(procs)
                time.sleep(0.5)

            hw3_tp = n / t_hw3 if t_hw3 > 0 else 0
            seq_tp = n / t_seq if t_seq > 0 else 0

            results.append({
                "dataset_size": n,
                "hw3_time_sec": round(t_hw3, 4),
                "hw3_throughput_rec_per_sec": round(hw3_tp, 1),
                "hw2_seq_time_sec": round(t_seq, 4),
                "hw2_seq_throughput_rec_per_sec": round(seq_tp, 1),
            })
            print(
                f"  -> N: {n:7,d} | HW3: {t_hw3:6.3f}s ({hw3_tp:9.1f} rec/s) | "
                f"HW2 Seq: {t_seq:6.3f}s ({seq_tp:9.1f} rec/s)"
            )

    # Save CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_file = RESULTS_DIR / "dataset_scaling.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "dataset_size", "hw3_time_sec", "hw3_throughput_rec_per_sec",
            "hw2_seq_time_sec", "hw2_seq_throughput_rec_per_sec"
        ])
        writer.writeheader()
        writer.writerows(results)
    print(f"[benchmark] Saved CSV results: {csv_file}")

    # Plot
    setup_plot_style()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    ns = [r["dataset_size"] for r in results]
    labels_k = [f"{n // 1000}K" for n in ns]
    hw3_times = [r["hw3_time_sec"] for r in results]
    seq_times = [r["hw2_seq_time_sec"] for r in results]
    hw3_tps = [r["hw3_throughput_rec_per_sec"] for r in results]
    seq_tps = [r["hw2_seq_throughput_rec_per_sec"] for r in results]

    # Subplot 1: Processing Time
    ax1.plot(ns, hw3_times, "o-", color="#1f77b4", linewidth=2.2, label=f"HW3 gRPC ({n_workers} Workers)")
    ax1.plot(ns, seq_times, "s--", color="#ff7f0e", linewidth=2.0, label="HW2 C++ Sequential Oracle")
    ax1.set_xlabel("Dataset Size (records)")
    ax1.set_ylabel("Total Processing Time (sec)")
    ax1.set_title("Processing Time vs Dataset Size")
    ax1.set_xticks(ns)
    ax1.set_xticklabels(labels_k)
    ax1.legend(loc="upper left")

    # Subplot 2: Throughput
    ax2.plot(ns, hw3_tps, "o-", color="#1f77b4", linewidth=2.2, label=f"HW3 gRPC ({n_workers} Workers)")
    ax2.plot(ns, seq_tps, "s--", color="#ff7f0e", linewidth=2.0, label="HW2 C++ Sequential Oracle")
    ax2.set_xlabel("Dataset Size (records)")
    ax2.set_ylabel("Throughput (records/sec)")
    ax2.set_title("Throughput vs Dataset Size")
    ax2.set_xticks(ns)
    ax2.set_xticklabels(labels_k)
    ax2.legend(loc="upper right")

    fig.tight_layout()
    plot_file = PLOTS_DIR / "dataset_scaling.png"
    fig.savefig(plot_file, dpi=200)
    plt.close(fig)
    print(f"[benchmark] Saved plot: {plot_file}")
    return results


# ── CLI Entry Point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HW3 Performance Benchmark Suite")
    parser.add_argument(
        "--experiment",
        choices=["scaling", "batch", "query", "size", "all"],
        default="all",
        help="Experiment to run: scaling, batch, query, size, or all",
    )
    parser.add_argument("--fast", action="store_true", help="Run with reduced dataset sizes for quick verification")
    args = parser.parse_args()

    n_records = 50_000 if args.fast else 100_000
    dataset_sizes = [10_000, 25_000, 50_000] if args.fast else [10_000, 50_000, 100_000, 250_000, 500_000]
    workers_list = [1, 2, 4] if args.fast else [1, 2, 4, 8]
    batch_sizes = [50, 200, 1000] if args.fast else [10, 50, 100, 500, 1000, 5000]
    client_counts = [0, 1, 2] if args.fast else [0, 1, 2, 4, 8]

    print("\n" + "#" * 65)
    print("  HW3 gRPC REAL-TIME WEATHER ANALYTICS — BENCHMARK SUITE")
    print(f"  Mode: {'FAST' if args.fast else 'STANDARD'}")
    print("#" * 65)

    if args.experiment in ("scaling", "all"):
        run_worker_scaling(n_records=n_records, workers_list=workers_list)

    if args.experiment in ("batch", "all"):
        run_batch_granularity(n_records=n_records, batch_sizes=batch_sizes)

    if args.experiment in ("query", "all"):
        run_query_concurrency(n_records=n_records, client_counts=client_counts)

    if args.experiment in ("size", "all"):
        run_dataset_scaling(dataset_sizes=dataset_sizes)

    print("\n" + "#" * 65)
    print("  ALL BENCHMARKS COMPLETED SUCCESSFULLY")
    print(f"  CSVs stored in:  {RESULTS_DIR}")
    print(f"  Plots stored in: {PLOTS_DIR}")
    print("#" * 65 + "\n")


if __name__ == "__main__":
    main()
