"""
hw3_grpc/common/aggregation.py — Aggregation/merge utilities for HW3.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Merge multiple worker-local AnalyticsSnapshot objects into a single
      global AnalyticsSnapshot (analogous to what MPI_Reduce does in HW2).
    - Provide a merge() function that can be called:
        * by the coordinator when collecting worker state updates
        * by correctness tests when comparing HW3 global result to HW2 reference
    - All merge operations must preserve the same tie-breaking rules as HW2.
    - Support incremental/streaming merge (worker updates arrive over time).

Notes:
    - Aggregation is separate from per-record analytics to allow the coordinator
      to aggregate across N workers without re-processing individual records.
"""

# TODO (implementation iteration): implement AnalyticsSnapshot merge logic
