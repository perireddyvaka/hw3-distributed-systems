# HW3 Section 2 Q2 — Submission Summary

**Generated:** 2026-09-29 21:30:30  
**Total runtime:** 2m 1s  
**Mode:** STANDARD

---

## Step Results

| Step | Status | Notes |
|---|:---:|---|
| `compile_hw2` | ✅ PASS | HW2 sequential oracle already compiled |
| `proto_gen` | ✅ PASS | Proto stubs generated |
| `unit_tests` | ✅ PASS | Unit tests: 27 tests passed → saved to submission/tests/unit_tests.txt |
| `streaming_tests` | ✅ PASS | Streaming tests: 4 passed → submission/tests/streaming_tests.txt |
| `concurrency_tests` | ✅ PASS | Concurrency tests: 1 passed → submission/tests/concurrency_tests.txt |
| `correctness_tests` | ✅ PASS | Correctness: 12/12 permutations PASSED → submission/tests/correctness_tests.txt |
| `benchmarks` | ✅ PASS | All 4 benchmark experiments completed |
| `memory_plot` | ✅ PASS | Memory usage plot saved → submission/benchmarks/plots/memory_usage.png |
| `live_run` | ✅ PASS | Live run captured → submission/live_run/ |

---

## Submission Folder Contents

```
submission/
├── SUBMISSION_SUMMARY.md          ← this file
├── README.md                      ← complete architecture, execution & analysis guide
├── proto/
│   └── weather.proto              ← Protocol Buffer service and message definitions
├── tests/
│   ├── unit_tests.txt             ← pytest: analytics & aggregation unit tests
│   ├── streaming_tests.txt        ← pytest: gRPC streaming integration tests
│   ├── concurrency_tests.txt      ← pytest: multi-client concurrency tests
│   └── correctness_tests.txt      ← pytest: 12-permutation HW2 oracle correctness
├── benchmarks/
│   ├── results/
│   │   ├── worker_scaling.csv     ← Exp 1: throughput vs worker count
│   │   ├── batch_granularity.csv  ← Exp 2: throughput vs batch size
│   │   ├── query_concurrency.csv  ← Exp 3: query latency under load
│   │   └── dataset_scaling.csv    ← Exp 4: HW3 gRPC vs HW2 C++ oracle
│   └── plots/
│       ├── worker_scaling.png     ← throughput & speedup charts
│       ├── batch_granularity.png  ← batch size vs throughput
│       ├── query_latency.png      ← latency percentiles & ingestion impact
│       ├── dataset_scaling.png    ← processing time & throughput vs N
│       └── memory_usage.png       ← peak RSS per worker configuration
└── live_run/
    ├── sample_query_output.txt    ← live analytics snapshot (HW3 gRPC)
    └── sample_oracle_output.txt   ← HW2 C++ oracle output (same dataset)
```

---

## How to Reproduce

```bash
# Full run from scratch (all tests + benchmarks + live demo)
bash run_submission.sh

# Fast mode (~5 min, reduced dataset sizes)
bash run_submission.sh --fast

# Tests + live run only (use cached benchmark CSVs/plots)
bash run_submission.sh --skip-benchmarks

# Benchmarks only
bash run_submission.sh --skip-tests --skip-live-run
```

---

## Key Results

| Metric | Value |
|---|---|
| Correctness tests | 12 / 12 permutations passed (< 10⁻⁵ float tolerance) |
| Peak streaming throughput | 735,862 rec/s (dataset scaling) / 617,620 rec/s (batch scaling) |
| Worker scaling speedup | 1.15× speedup at 2 workers (651,495 rec/s) |
| Median query latency | 2.58–3.54 ms under 1 to 8 concurrent clients |
| Tail query latency (p95) | < 5.7 ms across all query concurrency loads |
| Dataset scaling | 500K records in 0.70s (linear O(N)) |
