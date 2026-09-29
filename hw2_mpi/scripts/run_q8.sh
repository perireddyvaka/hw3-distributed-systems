#!/bin/bash
#SBATCH --job-name=q8_analytics
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=00:30:00
#SBATCH --output=q8_benchmark_%j.log
#SBATCH --partition=debug

module load hpcx-2.7.0/hpcx-ompi

echo "Checking correctness..."
bash verify_correctness.sh

echo "Running benchmarks..."
bash benchmark.sh