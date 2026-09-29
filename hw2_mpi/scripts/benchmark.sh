#!/bin/bash
OUT=results.csv
echo "size_label,N,K,S,P,time_seconds" > "$OUT"

mpicxx -O2 -std=c++17 -o q8_mpi q8_mpi.cpp

# Increased dataset sizes to properly stress-test the cluster
declare -A SIZES=(
    [small]="100000 10 100"
    [medium]="500000 20 500"
    [large]="2000000 50 1000"
    [very_large]="5000000 100 2000"
)

mkdir -p bench_data
for label in small medium large very_large; do
    read N K S <<< "${SIZES[$label]}"
    file="bench_data/${label}.txt"
    
    echo "Generating/checking dataset: $label ($N records)..."
    if [ ! -f "$file" ]; then
        python3 generate_dataset.py "$N" "$K" "$S" "$file" 42
    fi

    for p in 1 2 4 8; do
        best=""
        # Increased to 5 repetitions for smoother data
        for rep in 1 2 3 4 5; do
            t=$( (time mpirun --mca coll_hcoll_enable 0 --bind-to none --oversubscribe -np "$p" ./q8_mpi "$file" > /dev/null) 2>&1 | grep real | awk '{print $2}' | sed 's/m/:/g' | awk -F: '{print ($1*60)+$2}' )
            if [ -z "$best" ]; then 
                best="$t"
            else 
                best=$(awk -v a="$t" -v b="$best" 'BEGIN{print (a<b)?a:b}')
            fi
        done
        echo "$label,$N,$K,$S,$p,$best" | tee -a "$OUT"
    done
done