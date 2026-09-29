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

# Add __init__.py so generated package is importable
touch "$OUT_DIR/__init__.py"

echo "[generate_proto] Done. Generated files:"
ls "$OUT_DIR"
