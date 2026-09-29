"""
hw3_grpc/worker/worker_state.py — Worker-local analytics state.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Maintain a worker-local AnalyticsSnapshot for all records received
      by this worker.
    - Use the analytics logic from common/analytics.py to process each
      incoming WeatherRecord and update local state.
    - Expose the local snapshot for coordinator collection via GetLocalState.
    - Thread-safety is required if the worker gRPC server uses multiple threads.

Notes:
    - Worker state is a strict subset of global state.
    - The global state is formed by merging all worker states via
      common/aggregation.py — worker_state.py itself does NOT do global aggregation.
"""

# TODO (implementation iteration): implement WorkerLocalState class
