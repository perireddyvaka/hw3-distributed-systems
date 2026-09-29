#!/usr/bin/env python3
"""
hw3_grpc/benchmarks/generate_memory_plot.py
Generate memory usage plot from worker_scaling.csv.
Run from repository root:
    python3 hw3_grpc/benchmarks/generate_memory_plot.py
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_DIR = REPO_ROOT / "hw3_grpc" / "benchmarks" / "results"
PLOTS_DIR   = REPO_ROOT / "hw3_grpc" / "benchmarks" / "plots"

def main():
    csv_file = RESULTS_DIR / "worker_scaling.csv"
    rows = []
    with open(csv_file) as f:
        for r in csv.DictReader(f):
            rows.append(r)

    workers  = [int(r["workers"])          for r in rows]
    mem_mb   = [float(r["peak_memory_mb"]) for r in rows]
    tps      = [float(r["throughput_rec_per_sec"]) for r in rows]

    plt.style.use("seaborn-v0_8-whitegrid"
                  if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.labelsize": 11,
        "figure.dpi": 200,
        "grid.color": "#e0e0e0",
        "grid.linestyle": "--",
        "grid.alpha": 0.7,
    })

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # --- Memory vs workers ---
    ax1.bar(workers, mem_mb, color=["#1f77b4", "#2ca02c", "#d62728", "#9467bd"],
            width=0.6, alpha=0.85)
    ax1.set_xlabel("Number of Workers")
    ax1.set_ylabel("Peak RSS Memory (MB)")
    ax1.set_title("Peak Memory Usage vs Worker Count\n(N=100K records, Batch=500)")
    ax1.set_xticks(workers)
    for x, y in zip(workers, mem_mb):
        ax1.annotate(f"{y:.1f} MB", (x, y),
                     textcoords="offset points", xytext=(0, 5),
                     ha="center", fontsize=9)

    # --- Throughput vs memory (scatter) ---
    colours = ["#1f77b4", "#2ca02c", "#d62728", "#9467bd"]
    for x, y, w, c in zip(mem_mb, tps, workers, colours):
        ax2.scatter(x, y, color=c, s=100, zorder=5, label=f"{w} workers")
        ax2.annotate(f"{w}W", (x, y),
                     textcoords="offset points", xytext=(5, 4), fontsize=9)
    ax2.set_xlabel("Peak RSS Memory (MB)")
    ax2.set_ylabel("Throughput (records/sec)")
    ax2.set_title("Memory–Throughput Trade-off\nper Worker Configuration")
    ax2.legend(loc="lower right")

    fig.suptitle("HW3 gRPC — Memory Usage Analysis", fontsize=14, fontweight="bold", y=1.01)
    fig.tight_layout()

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / "memory_usage.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"[memory_plot] Saved → {out}")


if __name__ == "__main__":
    main()
