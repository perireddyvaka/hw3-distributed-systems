"""
hw3_grpc/worker/worker_server.py — Worker gRPC service.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Implement the WorkerService gRPC servicer (generated from weather.proto).
    - Expose endpoint(s):
        1. ProcessBatch: receives a batch of WeatherRecord messages from
           the coordinator, updates worker-local analytics state, and
           returns an acknowledgement.
        2. GetLocalState: returns the current worker-local AnalyticsSnapshot
           so the coordinator can aggregate it into the global state.
    - Each worker runs as an independent process on a unique port.
    - Worker identity (worker_id) and port are passed via command-line args.

Entry point:
    Will be runnable as: python -m hw3_grpc.worker.worker_server --id W --port PORT
"""

# TODO (implementation iteration): implement WorkerServicer class
# TODO (implementation iteration): implement serve() entry point
