#!/bin/bash
# comparison/run_comparison.sh
# End-to-end orchestration of correctness & performance comparison between HW2 (MPI) and HW3 (gRPC).
#
# Usage:
#   bash comparison/run_comparison.sh [--fast]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "================================================================="
echo "  HW2 (MPI) vs HW3 (gRPC) — End-to-End Comparison Pipeline"
echo "================================================================="

if [ -f "$REPO_ROOT/.venv/bin/python3" ]; then
    PYTHON="$REPO_ROOT/.venv/bin/python3"
elif [ -f "$REPO_ROOT/venv/bin/python3" ]; then
    PYTHON="$REPO_ROOT/venv/bin/python3"
else
    PYTHON="${PYTHON:-python3}"
fi

# 1. Compile C++ binaries if needed
if [ ! -f hw2_mpi/src/q8_seq ]; then
    echo "[compile] Compiling HW2 sequential oracle..."
    g++ -O2 -std=c++17 -o hw2_mpi/src/q8_seq hw2_mpi/src/q8_seq.cpp
fi

if [ ! -f hw2_mpi/src/q8_mpi ] && command -v mpicxx &>/dev/null; then
    echo "[compile] Compiling HW2 MPI binary..."
    mpicxx -O2 -std=c++17 -o hw2_mpi/src/q8_mpi hw2_mpi/src/q8_mpi.cpp
fi

# 2. Generate Protobuf stubs if needed
if [ ! -f hw3_grpc/generated/weather_pb2.py ]; then
    echo "[proto] Generating Protobuf and gRPC stubs..."
    bash hw3_grpc/scripts/generate_proto.sh
fi

# 3. Create required directories
mkdir -p comparison/datasets comparison/results/correctness comparison/results/performance comparison/plots

# 4. Generate shared reproducible test datasets
echo "[dataset] Generating shared reproducible datasets..."
"$PYTHON" -m hw3_grpc.dataset.generate_dataset 10000 10 50 comparison/datasets/small.txt 42
"$PYTHON" -m hw3_grpc.dataset.generate_dataset 50000 20 100 comparison/datasets/medium.txt 99

# Reset summary CSV
SUMMARY_CSV="comparison/results/comparison_summary.csv"
echo "label,ref_file,test_file,status,max_diff,tolerance" > "$SUMMARY_CSV"

# 5. Run Correctness Checks
echo "-----------------------------------------------------------------"
echo "  Step 1: Correctness Comparison across Paradigms"
echo "-----------------------------------------------------------------"

for DATASET_LABEL in "small" "medium"; do
    DATASET_FILE="comparison/datasets/${DATASET_LABEL}.txt"
    SEQ_OUT="comparison/results/correctness/${DATASET_LABEL}_seq.txt"
    MPI_OUT="comparison/results/correctness/${DATASET_LABEL}_mpi.txt"
    GRPC_OUT="comparison/results/correctness/${DATASET_LABEL}_grpc.txt"
    DIFF_FILE="comparison/results/correctness/${DATASET_LABEL}_diff.txt"

    echo "[eval] Processing dataset: ${DATASET_LABEL} (${DATASET_FILE})"

    # 1. Run HW2 sequential oracle
    ./hw2_mpi/src/q8_seq "$DATASET_FILE" > "$SEQ_OUT"

    # 2. Run HW2 MPI (if mpicxx and mpirun available)
    if [ -f hw2_mpi/src/q8_mpi ] && command -v mpirun &>/dev/null; then
        mpirun --mca coll_hcoll_enable 0 --bind-to none --oversubscribe -np 2 \
            ./hw2_mpi/src/q8_mpi "$DATASET_FILE" > "$MPI_OUT"
        "$PYTHON" comparison/compare_results.py "$SEQ_OUT" "$MPI_OUT" \
            --label "${DATASET_LABEL}_HW2_MPI_vs_Seq" \
            --diff-file "comparison/results/correctness/${DATASET_LABEL}_mpi_diff.txt" \
            --summary-csv "$SUMMARY_CSV"
    fi

    # 3. Run HW3 gRPC (2 workers)
    COORD_PORT=58000
    WORKER_BASE=58100
    K=10
    if [ "$DATASET_LABEL" = "medium" ]; then K=20; fi

    # Launch workers
    W0_PID=""
    W1_PID=""
    "$PYTHON" -m hw3_grpc.worker.worker_server --id 0 --port 58100 --k "$K" & W0_PID=$!
    "$PYTHON" -m hw3_grpc.worker.worker_server --id 1 --port 58101 --k "$K" & W1_PID=$!
    sleep 0.8

    # Launch coordinator
    COORD_PID=""
    "$PYTHON" -m hw3_grpc.coordinator.server --workers 2 --port "$COORD_PORT" --worker-base-port "$WORKER_BASE" --k "$K" & COORD_PID=$!
    sleep 1.2

    # Stream dataset
    "$PYTHON" -m hw3_grpc.client.streaming_client \
        --dataset "$DATASET_FILE" \
        --port "$COORD_PORT" \
        --batch-size 500 \
        --delay 0.0 > /dev/null

    # Query snapshot and write output
    "$PYTHON" -m hw3_grpc.client.query_client \
        --port "$COORD_PORT" \
        --raw > "$GRPC_OUT"

    # Terminate gRPC processes
    kill "$COORD_PID" "$W0_PID" "$W1_PID" 2>/dev/null || true
    wait "$COORD_PID" "$W0_PID" "$W1_PID" 2>/dev/null || true

    # Compare HW3 gRPC vs HW2 Seq
    "$PYTHON" comparison/compare_results.py "$SEQ_OUT" "$GRPC_OUT" \
        --label "${DATASET_LABEL}_HW3_gRPC_vs_Seq" \
        --diff-file "$DIFF_FILE" \
        --summary-csv "$SUMMARY_CSV"
done

# 6. Run Performance Comparison
echo "-----------------------------------------------------------------"
echo "  Step 2: Performance Benchmark Comparison"
echo "-----------------------------------------------------------------"
"$PYTHON" comparison/compare_benchmarks.py

echo "================================================================="
echo "  HW2 vs HW3 Comparison Complete!"
echo "  Correctness diffs: comparison/results/correctness/"
echo "  Summary table:     ${SUMMARY_CSV}"
echo "  Performance plots: comparison/plots/"
echo "================================================================="
