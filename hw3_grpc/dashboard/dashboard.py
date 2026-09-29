"""
hw3_grpc/dashboard/dashboard.py — CLI live analytics dashboard.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Periodically query the coordinator (via client/query_client.py) while
      ingestion is running.
    - Display a live, auto-refreshing CLI view of the current analytics state:
        * Aggregate metrics (averages, min/max, totals, extremes)
        * Top-K stations
        * Busiest interval
        * Ingestion progress (records processed, throughput estimate)
    - Query interval is configurable (from config.py / CLI flag).
    - Display must be graceful (does not crash if coordinator is not yet ready).

Entry point:
    Will be runnable as:
        python -m hw3_grpc.dashboard.dashboard [--interval SECONDS]
"""

# TODO (implementation iteration): implement Dashboard class with periodic refresh loop
# TODO (implementation iteration): implement main() entry point
