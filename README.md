# Weather Analytics — Distributed Systems HW2 + HW3

A complete distributed systems project implementing large-scale **weather and environmental data analytics** using two complementary parallel paradigms:

| Component | Paradigm | Status | Key Features |
|---|---|:---:|---|
| **HW2** | Sequential + MPI Batch Processing | ✅ Complete | C++17, MPI scatter/gather/reduce, reference oracle (`q8_seq.cpp`) |
| **HW3** | Real-Time Streaming Analytics with gRPC | ✅ Complete | Multi-worker gRPC streaming, round-robin batch dispatcher, single-pass $O(1)$ state updates, CLI dashboard, sub-4ms live queries |
| **Comparison** | Cross-Paradigm Evaluation | ✅ Complete | Automated correctness diffing against HW2 oracle, side-by-side performance scaling analysis, comparative plots |

---

## 1. Project Structure

```
weather-analytics/
├── hw2_mpi/          # HW2: Sequential + MPI batch analytics (C++17, OpenMPI)
│   ├── src/          # q8_seq.cpp (oracle), q8_mpi.cpp (MPI implementation)
│   ├── dataset/      # generate_dataset.py
│   ├── scripts/      # benchmark.sh, verify_correctness.sh, run_q8.sh, plot_results.py
│   └── results/      # results.csv (benchmark output)
├── hw3_grpc/         # HW3: Distributed Real-Time Streaming Analytics (Python 3, gRPC)
│   ├── proto/        # weather.proto (service definitions)
│   ├── coordinator/  # server.py, dispatcher.py (ingestion stream & query service)
│   ├── worker/       # worker_server.py (parallel partition state accumulators)
│   ├── client/       # streaming_client.py, query_client.py
│   ├── dashboard/    # dashboard.py (interactive terminal dashboard)
│   ├── common/       # analytics.py, aggregation.py (pure incremental logic)
│   ├── dataset/      # generate_dataset.py (reproducible datasets)
│   ├── tests/        # unit, concurrency, streaming & correctness tests
│   ├── benchmarks/   # run_benchmark.py, results/ (.csv), plots/ (.png)
│   ├── results/      # final_results/ (synced CSVs + plots), live_runs/ (sample outputs)
│   └── scripts/      # Orchestration and execution scripts
├── comparison/       # Cross-paradigm comparison layer
│   ├── compare_results.py      # Field-by-field correctness validator
│   ├── compare_benchmarks.py   # Performance comparison & plot generator
│   ├── run_comparison.sh       # Unified comparison pipeline
│   ├── results/                # correctness/ and performance/ summary CSVs
│   └── plots/                  # Comparative speedup and runtime plots
└── README.md         # Top-level documentation (this file)
```

> **For full HW3 documentation** — architecture, design decisions, execution walkthrough, correctness proof, and benchmark observations — see [`hw3_grpc/README.md`](hw3_grpc/README.md).

---

## 2. Single Entry Point — `run_submission.sh`

**This is the only command you need.** It runs everything and produces a `submission/` folder ready to submit as-is.

```bash
# Full run: all tests + all benchmarks + live demo  (~15-20 min)
bash run_submission.sh

# Fast mode: reduced dataset sizes  (~5 min)
bash run_submission.sh --fast

# Tests + live run only (uses cached benchmark results,  ~3 min)
bash run_submission.sh --skip-benchmarks

# Benchmarks only
bash run_submission.sh --skip-tests --skip-live-run
```

> **One-time pip prerequisite:** `pip install grpcio grpcio-tools protobuf pytest matplotlib pandas numpy psutil`  
> The script auto-compiles the HW2 oracle and generates proto stubs on first run.

### Output — `submission/` (submit this folder directly)

```
submission/
├── SUBMISSION_SUMMARY.md          ← auto-generated pass/fail report
├── tests/
│   ├── unit_tests.txt             ← pytest: analytics & aggregation unit tests
│   ├── streaming_tests.txt        ← pytest: gRPC streaming integration tests
│   ├── concurrency_tests.txt      ← pytest: multi-client concurrency tests
│   └── correctness_tests.txt      ← pytest: 12-permutation HW2 oracle correctness
├── benchmarks/
│   ├── results/                   ← 4 CSV files (one per experiment)
│   └── plots/                     ← 5 PNG files (including memory_usage.png)
└── live_run/
    ├── sample_query_output.txt    ← live analytics snapshot (HW3 gRPC)
    └── sample_oracle_output.txt   ← HW2 C++ oracle output (same dataset)
```

---

## 3. Live Demonstration: Full System Run

```bash
# Step 1: Generate dataset
python3 hw3_grpc/dataset/generate_dataset.py -n 100000 -k 10 -s 50 \
    -o hw3_grpc/data/weather_100k.txt --seed 42

# Step 2: Start cluster (1 coordinator + 4 workers)
bash hw3_grpc/scripts/start_system.sh --workers 4 --k 10

# Step 3: (New terminal) Live dashboard
python3 -m hw3_grpc.dashboard.dashboard --port 50050 --interval 0.5

# Step 4: (New terminal) Stream data
python3 -m hw3_grpc.client.streaming_client \
    --dataset hw3_grpc/data/weather_100k.txt --port 50050 \
    --batch-size 500 --delay 0.0

# Step 5: (New terminal) Query current analytics
python3 -m hw3_grpc.client.query_client --port 50050

# Step 6: Stop cluster
bash hw3_grpc/scripts/stop_system.sh
```

---

## 4. End-to-End Comparison: HW2 (MPI) vs HW3 (gRPC)

```bash
bash comparison/run_comparison.sh
```

This command:
1. Compiles HW2 sequential oracle (`q8_seq`) and MPI binary (`q8_mpi`).
2. Generates identical reproducible datasets in `comparison/datasets/`.
3. Executes all implementations and performs field-by-field diffing of all 14 weather metrics.
4. Records summary rows in `comparison/results/comparison_summary.csv`.
5. Generates side-by-side performance scaling plots in `comparison/plots/`.

---

## 5. Summary of System Analytics (HW2 Q8 Specification)

Both implementations strictly compute:
- `TOTAL_MEASUREMENTS`: Ingestion count
- `AVERAGE_TEMPERATURE`, `MIN_TEMPERATURE`, `MAX_TEMPERATURE`
- `AVERAGE_HUMIDITY`, `MIN_HUMIDITY`, `MAX_HUMIDITY`
- `AVERAGE_PRESSURE`, `MIN_PRESSURE`, `MAX_PRESSURE`
- `TOTAL_RAINFALL`, `MAX_RAINFALL`
- `AVERAGE_WIND_SPEED`, `MAX_WIND_SPEED`
- `EXTREME_TEMPERATURE_EVENTS`: Count where $T \ge 40^\circ\text{C}$ or $T \le 0^\circ\text{C}$
- `HOTTEST_MEASUREMENT` & `COLDEST_MEASUREMENT` (strict tie-breaking: temperature → timestamp → station ID)
- `BUSIEST_INTERVAL` (1-minute timestamp bucket with maximum count)
- `TOP_STATIONS`: Top $K$ stations ranked by count (descending), tie-broken by station ID, reporting count, average temperature, and total rainfall.

---

## 6. Benchmark Results Summary

| Experiment | Key Finding |
|---|---|
| **Worker Scaling** (N=100K, batch=500) | Peak at W=1 (495K rec/s); localhost IPC overhead grows with workers; memory scales ~44 MB/worker |
| **Batch Granularity** (N=100K, W=4) | Throughput: 26K rec/s (batch=10) → 565K rec/s (batch=5000); optimal balance at batch=500–1000 |
| **Query Concurrency** (N=100K, batch=500, W=4) | p50 latency ≤ 3.6 ms under 8 concurrent clients; ingestion throughput drops only ~8% from baseline |
| **Dataset Scaling** (W=4, batch=500) | HW3 processes 500K records in 1.27s (linear O(N)); HW2 C++ is 4–6× faster but lacks live querying |

For detailed observations and analysis, see [`hw3_grpc/README.md § 8`](hw3_grpc/README.md#8-performance-benchmark-results).