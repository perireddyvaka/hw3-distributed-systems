#!/usr/bin/env python3
"""
hw3_grpc/benchmarks/generate_memory_plot.py
Generate memory usage plot from worker_scaling.csv.

Can be called directly or invoked by run_submission.sh with overridden
RESULTS_DIR / PLOTS_DIR via module-level attribute injection.

Run from repository root:
    python3 hw3_grpc/benchmarks/generate_memory_plot.py
    python3 hw3_grpc/benchmarks/generate_memory_plot.py --output-dir submission/benchmarks
"""
import argparse
import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT   = Path(__file__).resolve().parent.parent.parent
RESULTS_DIR = REPO_ROOT / "hw3_grpc" / "benchmarks" / "results"
PLOTS_DIR   = REPO_ROOT / "hw3_grpc" / "benchmarks" / "plots"


def main(results_dir: Path = None, plots_dir: Path = None) -> None:
    """Generate memory usage bar chart + memory-throughput scatter.

    Args:
        results_dir: directory containing worker_scaling.csv.
                     Falls back to the module-level RESULTS_DIR.
        plots_dir:   directory to write memory_usage.png.
                     Falls back to the module-level PLOTS_DIR.
    """
    r_dir = results_dir or RESULTS_DIR
    p_dir = plots_dir   or PLOTS_DIR

    csv_file = r_dir / "worker_scaling.csv"
    if not csv_file.exists():
        print(f"[memory_plot] ERROR: {csv_file} not found.", file=sys.stderr)
        sys.exit(1)

    rows = []
    with open(csv_file) as f:
        for row in csv.DictReader(f):
            rows.append(row)

    workers = [int(r["workers"])               for r in rows]
    mem_mb  = [float(r["peak_memory_mb"])      for r in rows]
    tps     = [float(r["throughput_rec_per_sec"]) for r in rows]

    plt.style.use(
        "seaborn-v0_8-whitegrid"
        if "seaborn-v0_8-whitegrid" in plt.style.available
        else "default"
    )
    plt.rcParams.update({
        "font.family":       "sans-serif",
        "font.size":         11,
        "axes.titlesize":    13,
        "axes.titleweight":  "bold",
        "axes.labelsize":    11,
        "figure.dpi":        200,
        "grid.color":        "#e0e0e0",
        "grid.linestyle":    "--",
        "grid.alpha":        0.7,
    })

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # --- Bar: Memory vs worker count ---
    x_positions = list(range(len(workers)))
    bars = ax1.bar(x_positions, mem_mb, color="#1f77b4", width=0.6, alpha=0.85)
    ax1.set_xlabel("Worker Configuration")
    ax1.set_ylabel("Peak RSS Memory (MB)")
    ax1.set_title("Peak Memory Usage vs Worker Count\n(N=100K records, Batch=500)")
    ax1.set_xticks(x_positions)
    ax1.set_xticklabels([f"{w}W" for w in workers])
    for x, y in zip(x_positions, mem_mb):
        ax1.annotate(
            f"{y:.1f} MB", (x, y),
            textcoords="offset points", xytext=(0, 5),
            ha="center", fontsize=8,
        )

    # --- Scatter: Throughput vs Memory (trade-off) ---
    cmap = plt.get_cmap("tab10" if len(workers) <= 10 else "tab20")
    for idx, (x, y, w) in enumerate(zip(mem_mb, tps, workers)):
        c = cmap(idx % 10)
        ax2.scatter(x, y, color=c, s=110, zorder=5, label=f"{w} workers")
        ax2.annotate(
            f"{w}W", (x, y),
            textcoords="offset points", xytext=(5, 4), fontsize=9,
        )
    ax2.set_xlabel("Peak RSS Memory (MB)")
    ax2.set_ylabel("Throughput (records/sec)")
    ax2.set_title("Memory–Throughput Trade-off\nper Worker Configuration")
    ax2.legend(loc="lower right", fontsize=8, ncol=2)

    fig.suptitle(
        "HW3 gRPC — Memory Usage Analysis",
        fontsize=14, fontweight="bold", y=1.01,
    )
    fig.tight_layout()

    p_dir.mkdir(parents=True, exist_ok=True)
    out = p_dir / "memory_usage.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"[memory_plot] Saved → {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate memory usage plot from worker_scaling.csv")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Root directory (expects results/ and plots/ subdirs). "
             "Defaults to hw3_grpc/benchmarks/.",
    )
    args = parser.parse_args()

    if args.output_dir:
        out_root = Path(args.output_dir)
        main(results_dir=out_root / "results", plots_dir=out_root / "plots")
    else:
        main()
