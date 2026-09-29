"""hw3_grpc/generated package — exports generated protobuf & gRPC stubs."""

import sys
from pathlib import Path

_pkg_dir = str(Path(__file__).resolve().parent)
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)

try:
    from . import weather_pb2
    from . import weather_pb2_grpc
except ImportError:
    import weather_pb2
    import weather_pb2_grpc

__all__ = ["weather_pb2", "weather_pb2_grpc"]
