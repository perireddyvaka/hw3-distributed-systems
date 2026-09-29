"""
hw3_grpc/benchmarks/run_benchmark.py — HW3 performance benchmark runner.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    Run controlled performance experiments for HW3 gRPC analytics system.

    Experiments to be implemented:
        1. Worker scaling:
               Fix dataset size and batch size; vary NUM_WORKERS = [1, 2, 4, 8].
               Measure: total ingestion time, end-to-end processing time.

        2. Message batch granularity:
               Fix dataset size and worker count; vary BATCH_SIZE = [1, 10, 100, 1000].
               Measure: total ingestion time, streaming throughput (records/sec).

        3. Query frequency / concurrency:
               Fix ingestion; vary query interval and number of concurrent query clients.
               Measure: query latency (p50, p95, p99), throughput impact on ingestion.

        4. Dataset size scaling:
               Fix worker count and batch size; vary dataset size = [100K, 500K, 2M, 5M].
               Measure: total processing time, compare against HW2 MPI timing.

    Output:
        - results/ : CSV files with raw measurements per experiment
        - plots/   : matplotlib plots (speedup, throughput, latency)

    All benchmark parameters will be sourced from common/config.py.

Entry point (future):
    python hw3_grpc/benchmarks/run_benchmark.py --experiment [scaling|batch|query|size]
"""

# TODO (implementation iteration): implement benchmark runner
