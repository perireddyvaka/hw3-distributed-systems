#!/bin/bash
# hw3_grpc/scripts/generate_proto.sh
# Generate Python gRPC stubs from hw3_grpc/proto/weather.proto.
# Run from the repository root: bash hw3_grpc/scripts/generate_proto.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROTO_DIR="$REPO_ROOT/hw3_grpc/proto"
OUT_DIR="$REPO_ROOT/hw3_grpc/generated"

echo "[generate_proto] Generating Python gRPC stubs..."
echo "[generate_proto] Proto:  $PROTO_DIR/weather.proto"
echo "[generate_proto] Output: $OUT_DIR"

mkdir -p "$OUT_DIR"

python3 -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/weather.proto"

# Patch weather_pb2_grpc.py to allow both package and standalone import
python3 -c "
path = '$OUT_DIR/weather_pb2_grpc.py'
with open(path) as f:
    content = f.read()
if 'from . import weather_pb2 as weather__pb2' not in content:
    content = content.replace(
        'import weather_pb2 as weather__pb2',
        'try:\n    from . import weather_pb2 as weather__pb2\nexcept (ImportError, ValueError):\n    import weather_pb2 as weather__pb2'
    )
    with open(path, 'w') as f:
        f.write(content)
"

# Add __init__.py so generated package is cleanly importable
cat << 'EOF' > "$OUT_DIR/__init__.py"
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
EOF

echo "[generate_proto] Done. Generated files:"
ls "$OUT_DIR"
