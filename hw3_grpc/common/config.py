"""
hw3_grpc/common/config.py — System-wide configuration for HW3.

STATUS: PLACEHOLDER — values and structure will be finalized in next iteration.

Future responsibility:
    Centralised, single-source-of-truth configuration for all HW3 components.
    Expected configuration parameters (non-exhaustive):

    Coordinator:
        COORDINATOR_HOST      = "localhost"
        COORDINATOR_PORT      = 50051

    Workers:
        NUM_WORKERS           = 4          # number of worker processes
        WORKER_BASE_PORT      = 50100      # workers listen on BASE_PORT + worker_id

    Streaming client:
        STREAM_RATE_RPS       = None       # records-per-second; None = unlimited
        BATCH_SIZE            = 100        # records per gRPC message

    Dashboard / query client:
        QUERY_INTERVAL_SEC    = 2.0        # how often the dashboard polls

    Benchmarking:
        BENCHMARK_WORKER_COUNTS   = [1, 2, 4, 8]
        BENCHMARK_BATCH_SIZES     = [1, 10, 100, 1000]
        BENCHMARK_QUERY_INTERVALS = [0.5, 1.0, 2.0]

Notes:
    - All values here are approximate placeholders; exact defaults will be
      determined during the implementation iteration.
    - Secrets or environment-specific settings should NOT be hardcoded here.
"""

# TODO (implementation iteration): implement Config dataclass or constants
