"""
hw3_grpc/coordinator/server.py — gRPC coordinator service.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Implement the CoordinatorService gRPC servicer (generated from weather.proto).
    - Expose two endpoints:
        1. IngestStream: bi-directional or client-streaming RPC that receives
           WeatherRecord messages from the streaming client, batches them,
           and dispatches them to workers via dispatcher.py.
        2. QueryAnalytics: unary RPC that returns the current global
           AnalyticsResponse by reading coordinator/global state.
    - Manage the gRPC server lifecycle (start, graceful stop).
    - Coordinate with dispatcher.py for worker management.
    - Coordinate with state.py for the global analytics snapshot.

Entry point:
    Will be runnable as: python -m hw3_grpc.coordinator.server [--workers N] [--port PORT]
"""

# TODO (implementation iteration): implement CoordinatorServicer class
# TODO (implementation iteration): implement serve() entry point
