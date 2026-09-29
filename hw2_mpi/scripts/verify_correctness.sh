#!/bin/bash

# Compile the programs
mpicxx -O2 -std=c++17 -o q8_mpi q8_mpi.cpp
g++ -O2 -std=c++17 -o q8_seq q8_seq.cpp

# Define 5 rigorous test cases: (N K S SEED)
# Includes prime/odd N to test uneven MPI workload distribution
declare -a TESTS=(
    "1000 5 10 42"
    "5000 10 50 99"
    "10000 20 100 123"
    "50000 50 500 777"
    "99999 15 250 111" 
)

echo "========================================="
echo "Starting Rigorous Correctness Suite (5 Cases)"
echo "========================================="

test_num=1
for test_case in "${TESTS[@]}"; do
    read N K S SEED <<< "$test_case"
    echo "-----------------------------------------"
    echo "Test $test_num: N=$N, K=$K, S=$S (Seed: $SEED)"
    
    # Generate data
    python3 generate_dataset.py "$N" "$K" "$S" test_data.txt "$SEED"
    
    # Run Sequential Oracle
    ./q8_seq test_data.txt > seq_out.txt
    
    # Test across MPI process counts
    for p in 1 2 4 8; do
        mpirun --mca coll_hcoll_enable 0 --bind-to none --oversubscribe -np $p ./q8_mpi test_data.txt > mpi_out_$p.txt
        if diff -w seq_out.txt mpi_out_$p.txt > /dev/null; then
            echo "  [PASS] P=$p matched sequential output 100%"
        else
            echo "  [FAIL] P=$p output differed!"
        fi
    done
    ((test_num++))
done
echo "========================================="