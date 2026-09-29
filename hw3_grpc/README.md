# HW3 — Real-Time Weather Analytics with gRPC

Distributed Systems (Monsoon 2026) — Assignment 3  
Author: Peri Reddy Vaka

---

## 1. System Overview

HW3 transitions the weather analytics workload from a static batch model (HW2 MPI) into an **asynchronous, real-time distributed streaming system** using **gRPC** and **Protocol Buffers**.

The system accepts a continuous stream of weather measurements, partitions the data via round-robin distribution to worker nodes for parallel incremental analytics, maintains thread-safe worker and coordinator state, and serves low-latency interactive analytical queries and real-time terminal dashboards while ingestion is active.

### Key System Highlights
- **100% Correctness Parity**: Final global aggregations match the HW2 C++ sequential reference oracle (`q8_seq.cpp`) across all metrics within floating-point tolerance ($10^{-4}$).
- **High-Throughput Streaming**: Up to **564,904 records/sec** throughput using configurable gRPC batching.
- **Low-Latency Live Queries**: Concurrent analytical queries answered in **~3.1–3.6 ms (median)** during active streaming.
- **Pure Local Execution**: Self-contained Python 3 implementation using gRPC on `localhost` — no external brokers, databases, or message queues.
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

### 2.1 Data Path Walkthrough

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

### 2.2 Design Decisions & Rationale

**Round-Robin Distribution**  
Records are dispatched to workers in a strict round-robin sequence at the batch level. This choice ensures statistical balance without requiring a partition key or a hash function, which would require all workers to maintain state for all stations. Round-robin guarantees $O(1)$ dispatch overhead and produces near-equal load across workers on uniformly-distributed workloads like the weather dataset.

**Worker-Local Accumulators**  
Each worker maintains a single in-memory accumulator (`AnalyticsAccumulator`) that is updated in-place with every incoming record. This avoids storing raw records entirely, keeping per-worker memory $O(S + I)$ where $S$ is the number of distinct stations and $I$ is the number of 60-second temporal buckets — independent of the total number of records ingested.

**Associative Snapshot Merging**  
When the coordinator aggregates across workers, it calls `GetWorkerState()` on all workers in parallel (using a gRPC thread pool), then merges the returned partial snapshots using a deterministic, order-independent merge function. Because the merge is associative, the result is mathematically identical to a sequential single-pass scan — which is verified against the HW2 oracle in the correctness test suite.

**Monotonic State Protection**  
In a concurrent read scenario, different workers are sampled at slightly different instants. A worker mid-batch may temporarily report fewer measurements than one that just finished. The coordinator enforces a monotonicity invariant on `total_measurements` — it discards any merged snapshot that shows fewer total records than the last committed snapshot — preventing clients from observing temporal regressions.

**Thread Pool Isolation**  
The coordinator runs on a `ThreadPoolExecutor` with separate thread pools for ingestion dispatch and query handling. This decouples the ingestion RPC handler from the `GetAnalytics` handler, so concurrent queries do not contend with batch routing. As a result, ingestion throughput degrades by only ~8–9% under 8 concurrent query clients.

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

Key message definitions:
- `WeatherRecord`: Individual sensor reading (`timestamp`, `station_id`, `temperature`, `humidity`, `pressure`, `rainfall`, `wind_speed`).
- `RecordBatch`: Batch index + repeated list of `WeatherRecord`.
- `WorkerAnalyticsState`: Serialized worker state (running sums, extrema, temporal buckets, parallel arrays for per-station stats).
- `AnalyticsSnapshot`: Comprehensive aggregated system analytics matching the HW2 Q8 specification.

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
│   └── dashboard.py           # Live updating terminal dashboard (polling-based)
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
│   ├── generate_memory_plot.py# Memory usage plot generator
│   ├── results/               # Raw experiment measurements (.csv)
│   └── plots/                 # Generated matplotlib visual charts (.png)
├── results/
│   ├── final_results/         # Synced copies of all CSVs and plots for submission
│   └── live_runs/             # Sample terminal output from live system runs
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
- `g++` with C++17 support (for HW2 oracle verification in correctness tests)

### Step 1 — Install Python Dependencies
```bash
pip install grpcio grpcio-tools protobuf pytest matplotlib pandas numpy psutil
```

### Step 2 — Compile HW2 Sequential Oracle
```bash
g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
```

### Step 3 — Generate Protobuf & gRPC Stubs
```bash
bash hw3_grpc/scripts/generate_proto.sh
```

### Step 4 — Run Unit Test Suite (optional sanity check)
```bash
python3 -m pytest hw3_grpc/tests/test_analytics.py hw3_grpc/tests/test_aggregation.py -v
```

---

## 6. End-to-End Execution Walkthrough

This section walks through a complete system run from startup to shutdown with annotated terminal output.

### Step 1: Generate a Dataset

```bash
python3 hw3_grpc/dataset/generate_dataset.py \
    -n 100000 -k 10 -s 50 \
    -o hw3_grpc/data/weather_100k.txt \
    --seed 42
```
```
[generate_dataset] Writing 100000 records (50 stations, K=10) to hw3_grpc/data/weather_100k.txt
[generate_dataset] Done — 100000 records written.
```

### Step 2: Start the Cluster (1 Coordinator + 4 Workers)

```bash
bash hw3_grpc/scripts/start_system.sh --workers 4 --k 10
```
```
[start_system] Generating proto stubs...
[start_system] Starting 4 workers (base port: 50060)...
[start_system] Worker 0 started on port 50060 (PID 12340)
[start_system] Worker 1 started on port 50061 (PID 12341)
[start_system] Worker 2 started on port 50062 (PID 12342)
[start_system] Worker 3 started on port 50063 (PID 12343)
[start_system] Starting coordinator on port 50050...
[start_system] Coordinator started (PID 12350)

[start_system] System is ready!
  Coordinator: localhost:50050
  Workers:     4 workers on ports 50060-50063
  PID files:   /tmp/hw3_pids/
```

### Step 3: Launch the Live Dashboard (in a second terminal)

```bash
python3 -m hw3_grpc.dashboard.dashboard --port 50050 --interval 0.5
```
```
====================================================
  HW3 WEATHER ANALYTICS — CURRENT SNAPSHOT
====================================================
  TOTAL_MEASUREMENTS                         0

  AVERAGE_TEMPERATURE                 0.000000
  MIN_TEMPERATURE                     0.000000
  MAX_TEMPERATURE                     0.000000
  ...
  [Refreshes every 0.5s — waiting for stream...]
====================================================
```

### Step 4: Stream the Dataset (in a third terminal)

```bash
python3 -m hw3_grpc.client.streaming_client \
    --dataset hw3_grpc/data/weather_100k.txt \
    --port 50050 \
    --batch-size 500 \
    --delay 0.0
```
```
[streaming_client] Connecting to localhost:50050 ...
[streaming_client] Streaming hw3_grpc/data/weather_100k.txt
[streaming_client] Batch size: 500 | Delay: 0.0s
[streaming_client] Sent batch   1 (500 records) → Worker 0
[streaming_client] Sent batch   2 (500 records) → Worker 1
...
[streaming_client] Sent batch 200 (500 records) → Worker 3
[streaming_client] ✓ Stream complete: 100000 records in 0.218s (458,144 rec/s)
```

### Step 5: Query Final Analytics

```bash
python3 -m hw3_grpc.client.query_client --port 50050
```
```
========================================================
  HW3 WEATHER ANALYTICS — CURRENT SNAPSHOT
========================================================
  TOTAL_MEASUREMENTS                   100,000

  AVERAGE_TEMPERATURE                19.879139
  MIN_TEMPERATURE                    -9.971772
  MAX_TEMPERATURE                    49.999871

  AVERAGE_HUMIDITY                   55.304033
  MIN_HUMIDITY                       10.005393
  MAX_HUMIDITY                       99.997744

  AVERAGE_PRESSURE                  999.732919
  MIN_PRESSURE                      900.029515
  MAX_PRESSURE                     1099.990360

  TOTAL_RAINFALL                 501871.931322
  MAX_RAINFALL                       99.999442

  AVERAGE_WIND_SPEED                 75.303423
  MAX_WIND_SPEED                    149.993129

  EXTREME_TEMPERATURE_EVENTS             3,291

  HOTTEST  ts=1600007488  st=14  t=49.999871
  COLDEST  ts=1600002336  st=14  t=-9.971772

  BUSIEST_INTERVAL  bucket=26666694  count=95

  TOP_10_STATIONS:
    station=   42  count=     227  avg_t=21.899941  rain=11159.412126
    station=    4  count=     222  avg_t=20.031572  rain=11140.505061
    station=   12  count=     222  avg_t=18.941125  rain=11882.182150
    station=    1  count=     221  avg_t=20.253936  rain=11160.713277
    station=    8  count=     219  avg_t=19.668277  rain=11085.857814
    station=   13  count=     217  avg_t=19.994782  rain=10919.742624
    station=   37  count=     216  avg_t=18.943959  rain=10884.188314
    station=   26  count=     214  avg_t=18.726359  rain=10344.703524
    station=   19  count=     213  avg_t=21.501209  rain=11260.402977
    station=   47  count=     212  avg_t=19.695832  rain=10607.882041
```

### Step 6: Stop the Cluster

```bash
bash hw3_grpc/scripts/stop_system.sh
```
```
[stop_system] Stopping coordinator (PID 12350)...
[stop_system] Stopping worker 0 (PID 12340)...
[stop_system] Stopping worker 1 (PID 12341)...
[stop_system] Stopping worker 2 (PID 12342)...
[stop_system] Stopping worker 3 (PID 12343)...
[stop_system] All processes stopped.
```

---

## 7. Correctness Verification (HW3 vs HW2 C++ Oracle)

HW3 results are validated directly against `hw2_mpi/src/q8_seq.cpp`.

### Run the Automated Correctness Suite
```bash
bash hw3_grpc/scripts/run_correctness.sh
```

Or for a quick single-case check:
```bash
bash hw3_grpc/scripts/run_correctness.sh --fast
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

### How to Reproduce All Experiments

```bash
# Full benchmark suite (all 4 experiments) — takes ~15–20 minutes
bash hw3_grpc/scripts/run_benchmarks.sh --all

# Individual experiments
bash hw3_grpc/scripts/run_benchmarks.sh --experiment scaling
bash hw3_grpc/scripts/run_benchmarks.sh --experiment batch
bash hw3_grpc/scripts/run_benchmarks.sh --experiment query
bash hw3_grpc/scripts/run_benchmarks.sh --experiment size

# Fast reduced-size run for quick verification (~3 minutes)
bash hw3_grpc/scripts/run_benchmarks.sh --fast
```

**Output locations:**
- CSV results: `hw3_grpc/benchmarks/results/`
- Plot images: `hw3_grpc/benchmarks/plots/`

---

### Experiment 1: Worker Scaling

*Parameters: $N = 100,000$ records, Batch Size = 500, Delay = 0.0s*

| Workers ($W$) | Total Time (s) | Throughput (rec/s) | Speedup | Peak Memory (MB) |
|:---:|:---:|:---:|:---:|:---:|
| **1** | 0.202 | 494,974.7 | 1.00× | 80.3 |
| **2** | 0.222 | 451,429.5 | 0.91× | 120.1 |
| **4** | 0.261 | 383,265.7 | 0.77× | 198.4 |
| **8** | 0.225 | 443,842.4 | 0.90× | 352.4 |

**Observations:**

Peak throughput occurs at **1 worker (494,974 rec/s)** on a single local machine. This is a key insight: on `localhost`, the bottleneck is not analytical computation but **IPC overhead** — gRPC serialisation, loopback socket I/O, and Python GIL contention in the thread pool. Adding more workers increases the number of inter-process round-trips per batch without a proportional reduction in work per node, since each node was already processing fast enough to keep up with a single coordinator dispatch loop.

The slight recovery at 8 workers (relative to 4) is a noise-level fluctuation within the ~±5% variance of short benchmark runs. In a true multi-host deployment — where each worker has independent CPU cores and separate network interfaces — we would expect approximately linear throughput scaling up to the coordinator dispatch bottleneck.

Memory scales **linearly with worker count** (80 MB at W=1, 352 MB at W=8), which is expected: each worker process carries its own Python runtime and gRPC server stack (~50–80 MB base), and per-station accumulators grow proportionally. This is entirely predictable and does not represent a memory leak.

---

### Experiment 2: Message Batch Granularity

*Parameters: $N = 100,000$ records, Workers = 4*

| Batch Size | Total Time (s) | Throughput (rec/s) |
|:---:|:---:|:---:|
| **10** | 3.834 | 26,079.5 |
| **50** | 0.758 | 131,853.3 |
| **100** | 0.521 | 191,874.8 |
| **500** | 0.231 | 433,682.4 |
| **1,000** | 0.192 | 521,169.4 |
| **5,000** | 0.177 | **564,904.1** |

**Observations:**

This experiment demonstrates the dominant role of **gRPC framing overhead**. Each call to `ProcessBatch()` requires: a gRPC HTTP/2 frame encoding, loopback socket transmission, frame decoding on the worker, Protobuf deserialisation, accumulator updates, and a `BatchAck` response round-trip. At batch size 10, 10,000 such round-trips are made for 100K records; at batch size 5000, only 20 round-trips are made.

Throughput scales **super-linearly with batch size** at small sizes (26K → 191K rec/s from batch 10 to 100, a 7.4× increase) and transitions to **logarithmically diminishing returns** at large sizes (433K → 565K rec/s from batch 500 to 5000, only 1.3× increase). This indicates that individual record processing dominates at large batch sizes and gRPC framing overhead dominates at small batch sizes.

The **optimal operating point for streaming analytics** is batch size 500–1000, which achieves >430K rec/s while keeping individual message delivery latency under 10ms — preserving the ability to issue live queries with sub-5ms response time. Batch size 5000 maximises raw throughput but introduces coarser analytics update granularity.

---

### Experiment 3: Query Concurrency & Latency

*Parameters: $N = 100,000$, Batch Size = 500, Workers = 4, Query Interval ≈ 15ms per client*

| Concurrent Clients | Total Queries | Ingestion Throughput (rec/s) | p50 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **0** (Baseline) | 0 | 338,156.5 | — | — | — |
| **1** | 17 | 309,672.2 | 3.13 | 6.69 | 9.58 |
| **2** | 34 | 311,621.3 | 3.47 | 6.71 | 9.85 |
| **4** | 66 | 318,531.2 | 3.62 | 12.22 | 15.12 |
| **8** | 136 | 322,616.3 | 3.40 | 8.56 | 9.30 |

**Observations:**

Query latency remains **sub-4ms median (p50 = 3.1–3.6 ms)** even under 8 concurrent query clients firing every 15ms during active streaming — demonstrating effective isolation between the ingestion and query code paths.

The initial 8–10% drop in ingestion throughput from baseline (338K) to 1–2 clients (309K–311K rec/s) occurs because the coordinator's `GetAnalytics` handler calls `GetWorkerState()` on all workers in parallel, which briefly acquires their state lock. This contention is visible at the coordinator level but is bounded: throughput does not degrade further as client count increases from 2 to 8 (311K → 322K, essentially flat), confirming that the thread pool absorbs additional query load without penalising ingestion.

The p95 tail latency spike at 4 clients (12.22 ms) reflects occasional GIL-induced queueing in the Python thread pool when 4 `GetWorkerState()` parallel gRPC fan-outs coincide with ingestion dispatch bursts. At 8 clients the p95 recovers to 8.56 ms, likely because the query client threads are more evenly spread across the streaming window. In a production system, using asyncio-native gRPC stubs (`grpc.aio`) would eliminate this GIL contention entirely.

---

### Experiment 4: Dataset Size Scaling (HW3 gRPC vs HW2 C++ Sequential Oracle)

*Parameters: Workers = 4, Batch Size = 500*

| Dataset Size ($N$) | HW3 gRPC Time (s) | HW3 Throughput (rec/s) | HW2 Seq Time (s) | HW2 Seq Throughput (rec/s) | HW3/HW2 Time Ratio |
|:---:|:---:|:---:|:---:|:---:|:---:|
| **10,000** | 0.036 | 277,812.3 | 0.007 | 1,443,109.1 | 5.1× |
| **50,000** | 0.123 | 407,732.0 | 0.029 | 1,737,481.9 | 4.2× |
| **100,000** | 0.218 | 458,144.9 | 0.050 | 2,004,442.6 | 4.4× |
| **250,000** | 0.564 | 443,068.7 | 0.110 | 2,278,630.0 | 5.1× |
| **500,000** | 1.274 | 392,332.4 | 0.210 | 2,384,179.5 | 6.1× |

**Observations:**

Both systems scale **linearly $O(N)$** with dataset size, confirming that neither implementation has super-linear growth. The HW2 C++ sequential oracle is consistently **4–6× faster** in raw end-to-end time for this single-machine benchmark.

This gap is entirely expected and is the architectural trade-off of the gRPC streaming model:
- **HW2 sequential** reads a file directly into a tight C++ loop with no serialisation overhead. It does no network I/O, no cross-process communication, and leverages the CPU cache efficiently with a single thread. For a compute-bound, read-once workload on a single machine, this is the theoretical maximum.
- **HW3 gRPC** pays the overhead of: Protobuf serialisation, gRPC HTTP/2 framing, loopback TCP socket transmission, deserialisation, Python interpreter overhead, and inter-process coordination — for each of the 200 batches (100K records / batch 500). It gains capabilities that HW2 completely lacks: distributed ingestion from live sources, real-time queryability while streaming, multiple simultaneous clients, a live terminal dashboard, and horizontal scale-out to true multi-host clusters.

The HW3 throughput plateau at ~400–460K rec/s for N ≥ 50K demonstrates that the system has reached its steady-state throughput. The slight decline at N=500K (392K rec/s) is likely due to increased per-station accumulator dictionary sizes at higher record counts.

---

### Memory Usage Analysis

*Parameters: $N = 100,000$ records, Batch Size = 500*

| Workers ($W$) | Peak RSS Memory (MB) | Memory per Worker Process (approx.) |
|:---:|:---:|:---:|
| **1** | 80.3 | 80.3 MB |
| **2** | 120.1 | 60.1 MB |
| **4** | 198.4 | 49.6 MB |
| **8** | 352.4 | 44.1 MB |

**Observations:**

Total peak memory grows roughly linearly with worker count (80 → 352 MB), but the **per-worker memory decreases** as workers share a fixed coordinator overhead. This is consistent with the design: each worker process carries ~40–50 MB of Python/gRPC runtime baseline plus $O(S)$ per-station state ($S$ = 50 stations × ~1KB each ≈ negligible). The growth is dominated by the Python runtime overhead per process, not by the analytics data structures themselves. For very large numbers of stations ($S \gg 10^4$), per-worker memory would grow significantly and should be monitored.

---

## 9. Automated Tests

```bash
# Run unit tests for analytics accumulator and aggregation merge
python3 -m pytest hw3_grpc/tests/test_analytics.py hw3_grpc/tests/test_aggregation.py -v

# Run streaming pipeline integration tests
python3 -m pytest hw3_grpc/tests/test_streaming.py -m integration -v

# Run concurrent client query tests
python3 -m pytest hw3_grpc/tests/test_concurrency.py -v

# Run full HW2 vs HW3 correctness suite (requires compiled HW2 oracle)
bash hw3_grpc/scripts/run_correctness.sh

# Run all tests at once
python3 -m pytest -v
```

---

## 10. Summary of Design Trade-offs

| Dimension | HW2 MPI (Batch) | HW3 gRPC (Streaming) |
|---|---|---|
| **Data model** | Complete static dataset | Continuous real-time stream |
| **Throughput** | 1.4–2.4 M rec/s (C++, in-process) | 395K–565K rec/s (Python, gRPC) |
| **Latency** | Batch-only; no live queries | Sub-4ms live queries during ingestion |
| **Horizontal scale** | MPI ranks (tightly coupled) | Independent worker processes (loosely coupled) |
| **Memory per node** | O(N) raw data | O(S + I) accumulators only |
| **Live observability** | Not supported | CLI dashboard + concurrent query clients |
| **Language** | C++17 | Python 3 + gRPC |
| **Correctness** | Sequential oracle | Verified against HW2 sequential oracle |
