# Weather Analytics — Distributed Systems HW2 + HW3

A multi-iteration distributed systems project implementing large-scale
**weather and environmental data analytics** using two different parallel paradigms:

| Component | Paradigm | Status |
|-----------|----------|--------|
| **HW2** | Sequential + MPI batch processing | ✅ Complete |
| **HW3** | Real-time streaming analytics with gRPC | 🔲 Structure only (Iteration 1) |
| **Comparison** | HW2 vs HW3 correctness & performance | 🔲 Placeholder |

> **Iteration 1 establishes the repository structure only. HW2 is preserved as
> the existing baseline. HW3 implementation will be developed in subsequent iterations.**

---

## Project Structure

```
weather-analytics/
├── hw2_mpi/          # HW2: Sequential + MPI batch analytics (COMPLETE)
├── hw3_grpc/         # HW3: gRPC streaming analytics (STRUCTURE ONLY)
├── comparison/       # Cross-system comparison layer (PLACEHOLDER)
├── README.md         # This file
└── requirements.txt  # Top-level Python dependencies
```

---

## HW2 — Baseline: Sequential + MPI Batch Processing

Located in [`hw2_mpi/`](hw2_mpi/).

**What it does:**
- Reads a weather dataset in a single batch.
- Computes a comprehensive set of analytics (temperature, humidity, pressure,
  rainfall, wind speed statistics; hottest/coldest measurements; busiest time
  interval; top-K stations by record count).
- Sequential implementation (`q8_seq.cpp`) is the **correctness reference/oracle**.
- MPI implementation (`q8_mpi.cpp`) distributes computation across P processes
  using `MPI_Scatterv` for data distribution and `MPI_Reduce`/`MPI_Gather` for
  result aggregation.

**HW2 structure:**
```
hw2_mpi/
├── src/               # q8_seq.cpp (oracle), q8_mpi.cpp (MPI implementation)
├── dataset/           # generate_dataset.py
├── scripts/           # benchmark.sh, verify_correctness.sh, run_q8.sh, plot_results.py
├── data/              # datasets (gitignored)
├── results/           # results.csv (benchmark output from cluster run)
└── README.md          # HW2-specific instructions
```

See [`hw2_mpi/README.md`](hw2_mpi/README.md) for full HW2 usage instructions.

---

## HW3 — Extension: Real-Time gRPC Streaming Analytics

Located in [`hw3_grpc/`](hw3_grpc/).

> **Status: STRUCTURE ONLY — no implementation yet.**

**Planned architecture:**

```
Dataset → Streaming Client → Coordinator → Workers → Global Analytics State
                                                            ↓
                                              Query Client / Dashboard
```

**Key design constraints:**
- Uses the **same dataset format** as HW2 (enabling direct correctness comparison).
- Final analytics must **match HW2 sequential reference exactly**.
- Implemented in Python with gRPC / Protocol Buffers.
- No Kafka, Redis, PostgreSQL, or external infrastructure.
- Worker count is configurable.

See [`hw3_grpc/README.md`](hw3_grpc/README.md) for the planned HW3 structure.

---

## Comparison Layer

Located in [`comparison/`](comparison/).

> **Status: PLACEHOLDER — requires HW3 implementation to be useful.**

**Future purpose:**
- Run both HW2 and HW3 on identical reproducible datasets.
- Verify correctness: HW3 must match HW2 sequential oracle.
- Compare performance: MPI vs gRPC speedup, throughput, latency.

---

## Current Project Status

| Milestone | Status |
|-----------|--------|
| Iteration 1: Repository structure | ✅ Done |
| HW2 preservation | ✅ Done (checksums verified) |
| HW3 proto schema | 🔲 Placeholder |
| HW3 coordinator implementation | 🔲 Not started |
| HW3 worker implementation | 🔲 Not started |
| HW3 streaming client | 🔲 Not started |
| HW3 correctness tests | 🔲 Not started |
| HW3 benchmarks | 🔲 Not started |
| HW2 vs HW3 comparison | 🔲 Not started |

---

## Quick Start

### HW2 (already implemented)

See [`hw2_mpi/README.md`](hw2_mpi/README.md).

```bash
# Compile
g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
mpicxx -O2 -std=c++17 -o hw2_mpi/src/q8_mpi hw2_mpi/src/q8_mpi.cpp

# Generate a dataset
python3 hw2_mpi/dataset/generate_dataset.py 500000 20 500 medium.txt 42

# Run sequential
./hw2_mpi/src/q8_seq medium.txt

# Run MPI (4 processes)
mpirun -np 4 ./hw2_mpi/src/q8_mpi medium.txt
```

### HW3 (not yet implemented)

Implementation will be provided in the next iteration.

---

## Requirements

- **HW2**: MPI (e.g. OpenMPI or HPCX), g++/mpicxx, Python 3
- **HW3** (future): Python 3.9+, gRPC, protobuf (see [`hw3_grpc/requirements.txt`](hw3_grpc/requirements.txt))