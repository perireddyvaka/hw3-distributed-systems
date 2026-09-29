# HW3 — Real-Time Weather Analytics with gRPC

Distributed Systems (Monsoon 2026) — Assignment 3  
Author: Peri Reddy Vaka

---

## 1. System Overview

HW3 transitions the weather analytics workload from a static batch model (HW2 MPI) into an **asynchronous, real-time distributed streaming system** using **gRPC** and **Protocol Buffers**.

The system accepts a continuous stream of weather measurements, partitions the data via round-robin distribution to worker nodes for parallel incremental analytics, maintains thread-safe worker and coordinator state, and serves low-latency interactive analytical queries and real-time terminal dashboards while ingestion is active.

### Key System Highlights
- **100% Correctness Parity**: Final global aggregations produce outputs that strictly match the HW2 C++ sequential reference oracle (`q8_seq.cpp`) across all metrics within floating-point tolerance ($10^{-4}$).
- **High-Throughput Streaming**: Achieves up to **560,000+ records/sec** streaming throughput using configurable gRPC batching.
- **Low-Latency Live Queries**: Concurrent analytical queries are answered in **~3.1–3.6 ms (median)** while streaming ingestion is running.
- **Pure Local Execution**: Self-contained architecture in Python 3 with gRPC and Protobuf on `localhost` without external message brokers (no Kafka, Redis, or PostgreSQL).
- **Graceful Lifecycle Management**: Complete shell scripting for cluster orchestration, dataset generation, test suites, and automated benchmark pipelines.

---

## 2. Architecture & Communication Flow

```
                      +────────────────────────────────────────+
                      |         Weather Dataset File           |
                      |        (N records, K, S stations)      |
                      +────────────────────────────────────────+
                                          |
                                          v
                      +────────────────────────────────────────+
                      |            Streaming Client            |
                      |  (hw3_grpc.client.streaming_client)    |
                      +────────────────────────────────────────+
                                          |
                                          | gRPC Client-Streaming:
                                          | StreamMeasurements(stream RecordBatch)
                                          v
                      +────────────────────────────────────────+
                      |              Coordinator               |
                      |     (hw3_grpc.coordinator.server)      |
                      |                                        |
                      |   - Round-Robin Batch Dispatcher       |
                      |   - Global Analytics State Holder      |
                      +────────────────────────────────────────+
                             /            |            \
       gRPC ProcessBatch()  /             |             \  gRPC ProcessBatch()
                           v              v              v
                   +-------------+  +-------------+  +-------------+
                   |  Worker 0   |  |  Worker 1   |  | Worker N-1  |
                   | (Port 50060)|  | (Port 50061)|  | (Port 5006N)|
                   +-------------+  +-------------+  +-------------+
                   | Local State |  | Local State |  | Local State |
                   | Accumulator |  | Accumulator |  | Accumulator |
                   +-------------+  +-------------+  +-------------+
                           \              |              /
       gRPC GetWorkerState()\             |             /  gRPC GetWorkerState()
                             v            v            v
                      +────────────────────────────────────────+
                      |      Coordinator Aggregator / Merge    |
                      |     (hw3_grpc.common.aggregation)      |
                      +────────────────────────────────────────+
                                     /          \
                                    /            \
          gRPC GetAnalytics()      /              \  gRPC GetAnalytics()
                                  v                v
                      +-------------------+   +--------------------+
                      |   Query Client    |   | Terminal Dashboard |
                      | (query_client.py) |   |   (dashboard.py)   |
                      +-------------------+   +--------------------+
```

### Data Path Walkthrough

1. **Ingestion (Streaming)**:
   - The `StreamingClient` reads the space-delimited weather dataset line-by-line.
   - Records are grouped into chunks of size `BATCH_SIZE` (default: 500) and dispatched over an asynchronous gRPC client stream (`StreamMeasurements`).
   - The `Coordinator` receives each `RecordBatch` and dispatches it round-robin to one of $N$ worker nodes via `WorkerService.ProcessBatch()`.

2. **Worker Processing**:
   - Each `WorkerServer` receives batches and feeds records into its `WorkerLocalState`.
   - Analytics are computed **incrementally in single-pass $O(1)$ updates**:
     - Global sum, min, max for temperature, humidity, pressure, wind speed, rainfall.
     - Extreme temperature event counters ($T > 40.0^\circ\text{C}$ or $T < 0.0^\circ\text{C}$).
     - Hottest and coldest measurement records (breaking ties by earliest timestamp, then lowest station ID).
     - Temporal interval counts (bucketed into 60-second windows: `timestamp // 60`).
     - Per-station statistics (record count, temperature sum, rainfall sum).

3. **Aggregation & Querying**:
   - Query clients or the terminal dashboard issue `GetAnalytics` requests to the Coordinator.
   - The Coordinator queries all worker nodes in parallel via `WorkerService.GetWorkerState()`.
   - The coordinator's `merge()` engine combines worker summaries associatively and commutatively.
   - Monotonicity checks prevent state regressions during asynchronous multi-worker reads.
   - The response includes the top-$K$ busiest stations (sorted descending by count, ties broken by station ID ascending) and the overall busiest 1-hour interval.

---

## 3. Protocol Buffers Schema (`proto/weather.proto`)

```protobuf
syntax = "proto3";
package weather;

service CoordinatorService {
  rpc StreamMeasurements (stream RecordBatch) returns (StreamResponse);
  rpc GetAnalytics (AnalyticsRequest) returns (AnalyticsSnapshot);
}

service WorkerService {
  rpc ProcessBatch (RecordBatch) returns (BatchAck);
  rpc GetWorkerState (StateRequest) returns (WorkerAnalyticsState);
  rpc Reset (ResetRequest) returns (ResetResponse);
}
```

Key message definitions include:
- `WeatherRecord`: Represents an individual sensor reading (`timestamp`, `station_id`, `temperature`, `humidity`, `pressure`, `rainfall`, `wind_speed`).
- `RecordBatch`: Encapsulates a batch index and a repeated list of `WeatherRecord`.
- `WorkerAnalyticsState`: High-performance serialized worker state containing running sums, extrema, temporal buckets, and parallel arrays for per-station stats.
- `AnalyticsSnapshot`: Comprehensive aggregated system analytics matching the HW2 specification.

---

## 4. Directory & Module Structure

```
hw3_grpc/
├── common/
│   ├── __init__.py
│   ├── config.py              # Centralized configuration & environment variables
│   ├── models.py              # Data classes (WeatherRecord, MeasurementRef, StationStat, Snapshot)
│   ├── analytics.py           # Single-pass incremental accumulator (AnalyticsAccumulator)
│   └── aggregation.py         # Multi-worker snapshot merge logic
├── proto/
│   └── weather.proto          # Protocol Buffer definitions
├── generated/
│   ├── __init__.py
│   ├── weather_pb2.py         # Generated message classes
│   └── weather_pb2_grpc.py    # Generated gRPC client/server stubs
├── worker/
│   ├── __init__.py
│   ├── worker_state.py        # Thread-safe worker-local state wrapper
│   └── worker_server.py       # Worker gRPC service implementation
├── coordinator/
│   ├── __init__.py
│   ├── state.py               # Thread-safe global analytics state with monotonic protection
│   ├── dispatcher.py          # Round-robin batch dispatcher & worker state collector
│   └── server.py              # Coordinator gRPC service implementation
├── client/
│   ├── __init__.py
│   ├── streaming_client.py    # Ingestion client with batching, throttling, and limit controls
│   └── query_client.py        # CLI client for querying live and final analytics
├── dashboard/
│   ├── __init__.py
│   └── dashboard.py           # Live updating curses-style terminal dashboard
├── dataset/
│   ├── __init__.py
│   └── generate_dataset.py    # HW2-compatible reproducible dataset generator
├── tests/
│   ├── __init__.py
│   ├── test_analytics.py      # Unit tests for incremental analytics accumulator
│   ├── test_aggregation.py    # Unit tests for multi-snapshot merging
│   ├── test_streaming.py      # Integration tests for streaming pipeline
│   ├── test_concurrency.py    # Multi-client concurrent query tests
│   └── test_correctness.py    # End-to-end verification against HW2 C++ oracle
├── benchmarks/
│   ├── run_benchmark.py       # Performance experiment runner (scaling, batch, query, size)
│   ├── results/               # Raw experiment measurements (.csv)
│   └── plots/                 # Generated matplotlib visual charts (.png)
├── scripts/
│   ├── generate_proto.sh      # Compiles proto/weather.proto into generated/
│   ├── start_system.sh        # Starts 1 coordinator and N workers
│   ├── stop_system.sh         # Shuts down all running cluster processes
│   ├── run_correctness.sh     # Runs the full HW2 oracle correctness test suite
│   └── run_benchmarks.sh      # Runs the automated benchmark pipeline
├── requirements.txt           # Python package dependencies
└── README.md                  # This documentation
```

---

## 5. Setup & Installation

### Prerequisites
- Linux OS (tested on Ubuntu 24.04 LTS / x86_64)
- Python 3.10+
- `g++` with C++17 support (for HW2 oracle verification)

### Install Dependencies
```bash
pip install grpcio grpcio-tools protobuf pytest matplotlib pandas numpy
```

### Generate Protobuf Code
```bash
bash hw3_grpc/scripts/generate_proto.sh
```

---

## 6. Execution Guide

### 1. Generating a Test Dataset
```bash
# Generate 100,000 records with K=10, S=50 stations
python3 hw3_grpc/dataset/generate_dataset.py -n 100000 -k 10 -s 50 -o hw3_grpc/data/weather_100k.txt --seed 42
```

### 2. Starting the Cluster
```bash
# Start coordinator on port 50050 with 4 workers (ports 50060–50063)
bash hw3_grpc/scripts/start_system.sh --workers 4 --k 10
```

### 3. Monitoring with the Live Dashboard (Optional Terminal)
```bash
# Launch terminal dashboard polling every 0.5 seconds
python3 -m hw3_grpc.dashboard.dashboard --port 50050 --interval 0.5
```

### 4. Streaming Ingestion
```bash
# Stream dataset in batches of 500 with zero delay
python3 -m hw3_grpc.client.streaming_client \
    --dataset hw3_grpc/data/weather_100k.txt \
    --port 50050 \
    --batch-size 500 \
    --delay 0.0
```

### 5. Querying Current Analytics
```bash
# Query the coordinator for the latest analytics snapshot
python3 -m hw3_grpc.client.query_client --port 50050
```

### 6. Stopping the Cluster
```bash
bash hw3_grpc/scripts/stop_system.sh
```

---

## 7. Correctness Verification (HW3 vs HW2 C++ Oracle)

HW3 results are validated directly against `hw2_mpi/src/q8_seq.cpp`.

Run the automated correctness suite:
```bash
bash hw3_grpc/scripts/run_correctness.sh
```

### Test Coverage Matrix
All **12 / 12 test permutations passed**:

| Permutation | Dataset Size ($N$) | Stations ($S$) | Top-$K$ | Workers ($W$) | Result | Max Float Diff |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 1,000 | 10 | 5 | 1 | **PASSED** | $< 10^{-5}$ |
| 2 | 5,000 | 25 | 10 | 1 | **PASSED** | $< 10^{-5}$ |
| 3 | 10,000 | 50 | 10 | 1 | **PASSED** | $< 10^{-5}$ |
| 4 | 99,999 (prime) | 73 | 10 | 1 | **PASSED** | $< 10^{-5}$ |
| 5 | 1,000 | 10 | 5 | 2 | **PASSED** | $< 10^{-5}$ |
| 6 | 5,000 | 25 | 10 | 2 | **PASSED** | $< 10^{-5}$ |
| 7 | 10,000 | 50 | 10 | 2 | **PASSED** | $< 10^{-5}$ |
| 8 | 99,999 (prime) | 73 | 10 | 2 | **PASSED** | $< 10^{-5}$ |
| 9 | 1,000 | 10 | 5 | 4 | **PASSED** | $< 10^{-5}$ |
| 10 | 5,000 | 25 | 10 | 4 | **PASSED** | $< 10^{-5}$ |
| 11 | 10,000 | 50 | 10 | 4 | **PASSED** | $< 10^{-5}$ |
| 12 | 99,999 (prime) | 73 | 10 | 4 | **PASSED** | $< 10^{-5}$ |

Fields validated per run:
- Total measurements count
- Extreme temperature event count ($T > 40.0^\circ\text{C}$ or $T < 0.0^\circ\text{C}$)
- Temperature: min, max, avg
- Humidity: min, max, avg
- Pressure: min, max, avg
- Wind Speed: max, avg
- Rainfall: total, max
- Hottest measurement: timestamp, station ID, temperature
- Coldest measurement: timestamp, station ID, temperature
- Busiest 1-hour interval and measurement count
- Top-$K$ station rankings: station ID, measurement count, avg temperature, total rainfall

---

## 8. Performance Benchmark Results

The benchmark suite tests four core dimensions:
```bash
bash hw3_grpc/scripts/run_benchmarks.sh --all
```

All benchmark runs save raw CSV metrics to `hw3_grpc/benchmarks/results/` and publication plots to `hw3_grpc/benchmarks/plots/`.

### Experiment 1: Worker Scaling
*Parameters: $N = 100,000$, Batch Size = 500, delay = 0.0*

| Workers ($W$) | Total Time (s) | Ingestion Throughput (rec/s) | Relative Speedup |
|:---:|:---:|:---:|:---:|
| **1** | 0.229 | 436,841.5 | 1.00x |
| **2** | 0.216 | 462,480.6 | 1.06x |
| **4** | 0.245 | 408,916.5 | 0.94x |
| **8** | 0.263 | 379,780.0 | 0.87x |

**Analysis**:
- Peak throughput occurs at **2 workers (462,480 rec/s)** on local loopback.
- At 4 and 8 workers on a single host, IPC network serialization over localhost sockets becomes the bottleneck rather than worker computation, showing near-constant processing time (~0.22–0.26s for 100K records).

---

### Experiment 2: Message Batch Granularity
*Parameters: $N = 100,000$, Workers = 4*

| Batch Size | Total Time (s) | Throughput (rec/s) |
|:---:|:---:|:---:|
| **10** | 3.834 | 26,079.5 |
| **50** | 0.758 | 131,853.3 |
| **100** | 0.521 | 191,874.8 |
| **500** | 0.231 | 433,682.4 |
| **1000** | 0.192 | 521,169.4 |
| **5000** | 0.177 | **564,904.1** |

**Analysis**:
- Batching amortizes gRPC HTTP/2 frame overhead and context switching.
- Moving from fine granularity (batch 10) to coarse granularity (batch 5000) increases throughput by **21.6x** (from 26K to 565K records/sec).
- Diminishing returns occur beyond batch size 1000, establishing 500–1000 as the optimal balance between low streaming latency and high throughput.

---

### Experiment 3: Query Concurrency & Latency
*Parameters: $N = 100,000$, Batch Size = 500, Workers = 4, Query Interval = ~15ms*

| Concurrent Clients | Total Queries | Ingestion Throughput (rec/s) | Median Latency (p50) | Tail Latency (p95) |
|:---:|:---:|:---:|:---:|:---:|
| **0** (Baseline) | 0 | 338,156.5 | — | — |
| **1** | 17 | 309,672.2 | 3.13 ms | 6.69 ms |
| **2** | 34 | 311,621.3 | 3.47 ms | 6.71 ms |
| **4** | 66 | 318,531.2 | 3.62 ms | 12.22 ms |
| **8** | 136 | 322,616.3 | 3.40 ms | 8.56 ms |

**Analysis**:
- Query latency remains sub-4ms median ($3.13–3.62$ ms) even when 8 concurrent clients continuously query coordinator state during active streaming.
- Ingestion throughput is barely impacted (~310K–322K rec/s vs 338K rec/s baseline), demonstrating that asynchronous thread pools cleanly isolate query handling from the ingestion dispatch loop.

---

### Experiment 4: Dataset Size Scaling (HW3 gRPC vs HW2 C++ Sequential Oracle)
*Parameters: Workers = 4, Batch Size = 500*

| Dataset Size ($N$) | HW3 gRPC Time (s) | HW3 Throughput (rec/s) | HW2 Seq Time (s) | HW2 Seq Throughput (rec/s) |
|:---:|:---:|:---:|:---:|:---:|
| **10,000** | 0.036 | 277,812.3 | 0.007 | 1,443,109.1 |
| **50,000** | 0.123 | 407,732.0 | 0.029 | 1,737,481.9 |
| **100,000** | 0.218 | 458,144.9 | 0.050 | 2,004,442.6 |
| **250,000** | 0.564 | 443,068.7 | 0.110 | 2,278,630.0 |
| **500,000** | 1.274 | 392,332.4 | 0.210 | 2,384,179.5 |

**Analysis**:
- HW3 gRPC execution time scales linearly $O(N)$ with dataset size.
- The compiled C++ sequential binary is faster for raw batch computation in memory, but lacks distributed scale-out, network ingestion, live query capability, and real-time dashboarding.
- HW3 processes 500,000 records end-to-end across a 5-node distributed gRPC cluster in just **1.27 seconds**.

---

## 9. Design Decisions & Trade-Offs

1. **Worker-Local Accumulators**:
   - Rather than storing raw records in memory, each worker updates running sums, extrema, and counts in place ($O(1)$ space and time per record).
   - This prevents memory bloat and allows processing multi-gigabyte streams without running out of RAM.

2. **Associative Merging**:
   - The coordinator never performs per-record operations. It merges pre-aggregated worker snapshots in $O(S + I)$ where $S$ is station count and $I$ is temporal intervals.

3. **Read Monotonicity**:
   - In concurrent distributed systems, interleaved polling of workers could yield temporary regressions if one worker is sampled mid-batch.
   - The coordinator protects `GlobalAnalyticsState` by enforcing non-decreasing measurement totals, ensuring clients always observe monotonic forward progress.

4. **Batching vs Latency Trade-Off**:
   - Record batches are tuned to 500 records by default. This delivers over 400K rec/s throughput while keeping message delivery latency under 10ms.

---

## 10. Automated Tests

```bash
# Run unit tests
python3 -m pytest hw3_grpc/tests/test_analytics.py hw3_grpc/tests/test_aggregation.py -v

# Run streaming pipeline tests
python3 -m pytest hw3_grpc/tests/test_streaming.py -m integration -v

# Run concurrent client query tests
python3 -m pytest hw3_grpc/tests/test_concurrency.py -v

# Run full HW2 vs HW3 correctness suite
bash hw3_grpc/scripts/run_correctness.sh
```
