"""
hw3_grpc/coordinator/dispatcher.py — Record dispatcher from coordinator to workers.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Maintain a pool of gRPC stubs to all registered worker services.
    - Distribute incoming WeatherRecord batches across workers.
    - Initial distribution strategy: round-robin over NUM_WORKERS workers.
    - Support configurable worker count (from config.py).
    - Handle worker failures gracefully (retry / skip dead workers).

Notes:
    - The dispatcher is called by coordinator/server.py inside the IngestStream handler.
    - It does NOT aggregate state — that is done by coordinator/state.py after
      workers respond with their updated local analytics.
"""

# TODO (implementation iteration): implement Dispatcher class
