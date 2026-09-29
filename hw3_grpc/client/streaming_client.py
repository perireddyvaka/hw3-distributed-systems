"""hw3_grpc/client/streaming_client.py — Dataset replay streaming client.

Reads a pre-generated dataset file and streams records to the coordinator
via the gRPC StreamMeasurements RPC.

Usage:
    python -m hw3_grpc.client.streaming_client \\
        --dataset hw3_grpc/data/generated/data.txt \\
        [--host localhost] [--port 50050] \\
        [--batch-size 100] [--delay 0.0] [--limit 0]
"""

from __future__ import annotations
import argparse
import logging
import sys
import time
from typing import Iterator

import grpc

sys.path.insert(0, "hw3_grpc/generated")
import weather_pb2
import weather_pb2_grpc

from hw3_grpc.common import config as cfg
from hw3_grpc.common.models import WeatherRecord

log = logging.getLogger("streaming_client")


def _parse_dataset(path: str):
    """Parse dataset file; yield (n, k, s) header then WeatherRecord objects."""
    with open(path) as fh:
        header = fh.readline().strip().split()
        n, k, s = int(header[0]), int(header[1]), int(header[2])
        yield n, k, s
        for line in fh:
            line = line.strip()
            if line:
                yield WeatherRecord.from_line(line)


def _batch_generator(
    path: str, batch_size: int, delay: float, limit: int
) -> Iterator[weather_pb2.RecordBatch]:
    """Yield RecordBatch proto messages from the dataset."""
    gen = _parse_dataset(path)
    n, k, s = next(gen)  # consume header
    log.info("Dataset: N=%d K=%d S=%d  batch_size=%d  delay=%.3fs", n, k, s, batch_size, delay)

    batch_records: list[weather_pb2.WeatherRecord] = []
    batch_index = 0
    sent = 0

    for rec in gen:
        if limit > 0 and sent >= limit:
            break
        batch_records.append(
            weather_pb2.WeatherRecord(
                timestamp=rec.timestamp,
                station_id=rec.station_id,
                temperature=rec.temperature,
                humidity=rec.humidity,
                pressure=rec.pressure,
                rainfall=rec.rainfall,
                wind_speed=rec.wind_speed,
            )
        )
        sent += 1

        if len(batch_records) >= batch_size:
            yield weather_pb2.RecordBatch(records=batch_records, batch_index=batch_index)
            batch_index += 1
            batch_records = []
            if delay > 0:
                time.sleep(delay)

    # flush remaining
    if batch_records:
        yield weather_pb2.RecordBatch(records=batch_records, batch_index=batch_index)


def stream(
    dataset_path: str,
    host: str = cfg.COORDINATOR_HOST,
    port: int = cfg.COORDINATOR_PORT,
    batch_size: int = cfg.BATCH_SIZE,
    delay: float = cfg.STREAM_DELAY,
    limit: int = 0,
) -> dict:
    """Stream the dataset to the coordinator. Returns statistics dict."""
    address = f"{host}:{port}"
    log.info("Connecting to coordinator at %s", address)

    channel = grpc.insecure_channel(address, options=cfg.GRPC_OPTIONS)
    stub = weather_pb2_grpc.CoordinatorServiceStub(channel)

    t_start = time.perf_counter()
    try:
        response = stub.StreamMeasurements(
            _batch_generator(dataset_path, batch_size, delay, limit),
            timeout=3600,
        )
    finally:
        channel.close()

    elapsed = time.perf_counter() - t_start
    throughput = response.records_received / elapsed if elapsed > 0 else 0

    stats = {
        "success": response.success,
        "records_received": response.records_received,
        "elapsed_sec": round(elapsed, 4),
        "throughput_rps": round(throughput, 1),
        "message": response.message,
    }
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="HW3 gRPC Streaming Client")
    parser.add_argument("--dataset", required=True, help="Path to dataset file")
    parser.add_argument("--host", default=cfg.COORDINATOR_HOST)
    parser.add_argument("--port", type=int, default=cfg.COORDINATOR_PORT)
    parser.add_argument("--batch-size", type=int, default=cfg.BATCH_SIZE)
    parser.add_argument("--delay", type=float, default=cfg.STREAM_DELAY,
                        help="Seconds to sleep between batches (0=no delay)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max records to stream (0=all)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [StreamingClient] %(levelname)s %(message)s",
    )

    stats = stream(
        dataset_path=args.dataset,
        host=args.host,
        port=args.port,
        batch_size=args.batch_size,
        delay=args.delay,
        limit=args.limit,
    )
    print("\n=== Streaming Complete ===")
    print(f"  Records sent    : {stats['records_received']:,}")
    print(f"  Elapsed         : {stats['elapsed_sec']:.3f}s")
    print(f"  Throughput      : {stats['throughput_rps']:,.0f} records/sec")
    print(f"  Success         : {stats['success']}")


if __name__ == "__main__":
    main()
