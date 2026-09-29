#!/bin/bash
# hw3_grpc/scripts/run_benchmarks.sh
# Run the complete suite of HW3 gRPC performance benchmark experiments.
#
# Usage:
#   bash hw3_grpc/scripts/run_benchmarks.sh [--all | --fast | --experiment <scaling|batch|query|size>]
#
# Flags:
#   --all         Run all 4 experiments with standard sizes (default)
#   --fast        Run all 4 experiments with reduced sizes for fast verification
#   --experiment  Run a specific experiment: scaling, batch, query, or size

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "================================================================="
echo "  HW3 gRPC Real-Time Weather Analytics — Benchmark Suite"
echo "================================================================="

# Compile HW2 seq if needed for comparison
if [ ! -f hw2_mpi/src/q8_seq ]; then
    echo "[run_benchmarks] Compiling HW2 sequential oracle..."
    g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
fi

# Generate protobuf stubs if needed
if [ ! -f hw3_grpc/generated/weather_pb2.py ]; then
    bash hw3_grpc/scripts/generate_proto.sh
fi

mkdir -p hw3_grpc/benchmarks/results hw3_grpc/benchmarks/plots

ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--all" ]; then
        ARGS+=(--experiment all)
    else
        ARGS+=("$arg")
    fi
done

if [ -f "$REPO_ROOT/.venv/bin/python3" ]; then
    PYTHON="$REPO_ROOT/.venv/bin/python3"
elif [ -f "$REPO_ROOT/venv/bin/python3" ]; then
    PYTHON="$REPO_ROOT/venv/bin/python3"
else
    PYTHON="${PYTHON:-python3}"
fi

"$PYTHON" hw3_grpc/benchmarks/run_benchmark.py "${ARGS[@]}"

echo "================================================================="
echo "  Benchmark suite finished."
echo "  CSV results:  hw3_grpc/benchmarks/results/"
echo "  Plots saved:  hw3_grpc/benchmarks/plots/"
echo "================================================================="
