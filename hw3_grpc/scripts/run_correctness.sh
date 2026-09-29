#!/bin/bash
# hw3_grpc/scripts/run_correctness.sh
# Run the end-to-end HW3 vs HW2 correctness verification.
#
# Prerequisites:
#   - HW2 sequential binary compiled:
#       g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
#   - Python deps installed: pip install grpcio grpcio-tools pytest
#   - Proto generated: bash hw3_grpc/scripts/generate_proto.sh
#
# Usage:
#   bash hw3_grpc/scripts/run_correctness.sh [--fast]
#   --fast: run only 1 small test case with 1-2 workers

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

FAST=${1:-""}

echo "========================================"
echo "  HW3 Correctness Verification"
echo "========================================"

# Compile HW2 seq if needed
if [ ! -f hw2_mpi/src/q8_seq ]; then
    echo "[correctness] Compiling HW2 sequential oracle..."
    g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
fi

# Generate proto if needed
if [ ! -f hw3_grpc/generated/weather_pb2.py ]; then
    bash hw3_grpc/scripts/generate_proto.sh
fi

if [ -f "$REPO_ROOT/.venv/bin/python3" ]; then
    PYTHON="$REPO_ROOT/.venv/bin/python3"
elif [ -f "$REPO_ROOT/venv/bin/python3" ]; then
    PYTHON="$REPO_ROOT/venv/bin/python3"
else
    PYTHON="${PYTHON:-python3}"
fi

if [ "$FAST" = "--fast" ]; then
    echo "[correctness] Running FAST subset (1 test case x 2 worker configs)..."
    "$PYTHON" -m pytest hw3_grpc/tests/test_correctness.py \
        -m "integration and correctness" \
        -k "small_1k and (1- or 2-)" \
        -v --tb=short
else
    echo "[correctness] Running FULL correctness suite (4 cases x 3 worker configs)..."
    "$PYTHON" -m pytest hw3_grpc/tests/test_correctness.py \
        -m "integration and correctness" \
        -v --tb=short
fi

echo ""
echo "[correctness] Complete."
