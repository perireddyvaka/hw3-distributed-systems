"""
hw3_grpc/client/streaming_client.py — Dataset replay streaming client.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    - Read a pre-generated weather dataset file (same format as HW2).
    - Replay records as a live gRPC stream to the coordinator's IngestStream RPC.
    - Configurable streaming rate (records-per-second; unlimited by default).
    - Configurable batch/message granularity (records per gRPC message send).
    - Track ingestion progress and report throughput metrics.
    - Signal the coordinator when the dataset replay is complete.

Dataset format (same as HW2):
    Line 1: N K S
    Lines 2..N+1: timestamp station_id temperature humidity pressure rainfall wind_speed

Entry point:
    Will be runnable as:
        python -m hw3_grpc.client.streaming_client \\
            --dataset path/to/data.txt \\
            [--rate RPS] \\
            [--batch-size N]
"""

# TODO (implementation iteration): implement StreamingClient class
# TODO (implementation iteration): implement main() entry point
