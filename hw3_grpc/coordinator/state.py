"""
hw3_grpc/coordinator/state.py — Coordinator global analytics state.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Maintain the global AnalyticsSnapshot that represents the aggregated
      result of all records processed by all workers so far.
    - Provide thread-safe read (for query RPCs) and write (for worker updates) access.
    - Update global state by merging incoming worker-local state snapshots
      using the merge logic in common/aggregation.py.
    - Expose a get_snapshot() method for the QueryAnalytics RPC handler.

Notes:
    - State is kept in-memory only (no persistence in this implementation).
    - Thread-safety is required because IngestStream and QueryAnalytics may
      run concurrently in the gRPC server thread pool.
"""

# TODO (implementation iteration): implement GlobalAnalyticsState class with thread-safe access
