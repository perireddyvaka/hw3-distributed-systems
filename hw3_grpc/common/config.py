"""hw3_grpc/common/config.py — centralised configuration for HW3."""

import os

# ── Coordinator ───────────────────────────────────────────────────────────────
COORDINATOR_HOST: str = os.environ.get("COORDINATOR_HOST", "localhost")
COORDINATOR_PORT: int = int(os.environ.get("COORDINATOR_PORT", "50050"))

# ── Workers ───────────────────────────────────────────────────────────────────
WORKER_COUNT: int = int(os.environ.get("WORKER_COUNT", "4"))
WORKER_BASE_PORT: int = int(os.environ.get("WORKER_BASE_PORT", "50060"))
# Worker i listens on WORKER_BASE_PORT + i

# ── Streaming client ──────────────────────────────────────────────────────────
STREAM_DELAY: float = float(os.environ.get("STREAM_DELAY", "0.0"))
# seconds between batches; 0 = as fast as possible
BATCH_SIZE: int = int(os.environ.get("BATCH_SIZE", "100"))
# records per gRPC message

# ── Query / dashboard ─────────────────────────────────────────────────────────
QUERY_INTERVAL: float = float(os.environ.get("QUERY_INTERVAL", "1.0"))
# seconds between dashboard refresh

# ── gRPC options ──────────────────────────────────────────────────────────────
MAX_MESSAGE_LENGTH: int = 100 * 1024 * 1024  # 100 MB
GRPC_OPTIONS = [
    ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
    ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
]

# ── Benchmarking ──────────────────────────────────────────────────────────────
BENCHMARK_WORKER_COUNTS = [1, 2, 4, 8]
BENCHMARK_BATCH_SIZES = [1, 10, 100, 1000]
BENCHMARK_QUERY_CLIENTS = [0, 1, 2, 4]
BENCHMARK_DATASET_SIZES = [100_000, 500_000, 2_000_000]


def worker_address(worker_id: int, host: str = COORDINATOR_HOST) -> str:
    """Return the gRPC address string for a given worker id."""
    return f"{host}:{WORKER_BASE_PORT + worker_id}"


def coordinator_address(host: str = COORDINATOR_HOST, port: int = COORDINATOR_PORT) -> str:
    return f"{host}:{port}"
