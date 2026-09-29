# HW3 — Real-Time Weather Analytics with gRPC

> **Status: STRUCTURE ONLY — implementation will be developed in subsequent iterations.**

## Overview

HW3 extends the batch-processing weather analytics system from HW2 into a
real-time streaming analytics system using **gRPC** and **Protocol Buffers**.

The system ingests a pre-generated weather dataset as a live stream, distributes
records across multiple worker processes for parallel analytics, and exposes
live query and dashboard access while ingestion is running.

Final HW3 analytics results must exactly match the HW2 sequential reference
(`q8_seq.cpp`) for the same dataset.

## Architecture

```
Dataset File
     |
     v
Streaming Client          ← reads dataset, replays as gRPC stream
     |
     | gRPC stream (WeatherRecord messages)
     v
Coordinator               ← receives stream, dispatches to workers
     |
     +-------------------+-------------------+
     |                   |                   |
     v                   v                   v
 Worker 0            Worker 1  ...       Worker N-1
     |                   |                   |
     +-------------------+-------------------+
                         |
                         v
               Global Analytics State
                         |
                    +----+----+
                    |         |
                    v         v
              Query        Dashboard
              Client        CLI
```

## Components (All Placeholders — Not Yet Implemented)

| Directory         | Responsibility                                               |
|-------------------|--------------------------------------------------------------|
| `proto/`          | Protocol Buffer schema (`weather.proto`)                    |
| `generated/`      | Auto-generated gRPC Python stubs (not committed)            |
| `common/`         | Shared models, analytics logic, aggregation, config         |
| `coordinator/`    | gRPC coordinator service, dispatcher, global state          |
| `worker/`         | gRPC worker service, worker-local analytics state           |
| `client/`         | Streaming client (dataset replay), query client             |
| `dashboard/`      | CLI live analytics dashboard                                |
| `dataset/`        | HW3 dataset generator (same format as HW2)                  |
| `tests/`          | Unit and integration tests                                   |
| `benchmarks/`     | Performance experiment runner                               |
| `scripts/`        | System lifecycle and benchmark shell scripts                |
| `data/`           | Generated datasets and sample data                          |
| `results/`        | Final analytics results and live-run snapshots              |

## Dataset Format

HW3 uses the **exact same format as HW2**:

```
N K S
timestamp station_id temperature humidity pressure rainfall wind_speed
```

This ensures correctness can be validated against the HW2 sequential oracle.

## Requirements

See [`requirements.txt`](requirements.txt).

Install (future, after implementation):

```bash
pip install -r hw3_grpc/requirements.txt
```

## Running (Future — Not Yet Implemented)

```bash
# 1. Generate proto stubs
bash hw3_grpc/scripts/generate_proto.sh

# 2. Start system (coordinator + workers)
bash hw3_grpc/scripts/start_system.sh

# 3. Stream a dataset
python -m hw3_grpc.client.streaming_client --dataset data/generated/medium.txt

# 4. Query analytics
python -m hw3_grpc.client.query_client

# 5. Stop system
bash hw3_grpc/scripts/stop_system.sh
```

## Correctness Validation (Future)

```bash
bash hw3_grpc/scripts/run_correctness.sh
```

## Benchmarks (Future)

```bash
bash hw3_grpc/scripts/run_benchmarks.sh
```
