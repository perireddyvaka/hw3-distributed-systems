# Q8. Large-Scale Weather and Environmental Data Analytics (MPI)

## Files Included
- `q8_mpi.cpp` : The distributed MPI solution. Rank 0 reads and scatters chunks of size `N/P` to all ranks (via `MPI_Scatterv` on a packed `Record` struct). Ranks compute local sums/max/min/extreme-event counts/per-station stats/busiest-interval hashmap, then reduce (`MPI_Reduce`) and gather (`MPI_Gather`/`MPI_Gatherv`) back to Rank 0, which merges and prints the final report.
- `q8_seq.cpp` : The sequential implementation used as the correctness oracle. Reads the same input format and prints the same report format for diffing.
- `generate_dataset.py` : Produces a randomized dataset — usage: `python3 generate_dataset.py <N> <K> <S> <output_file> [seed]` (records: timestamp, station, temp, humidity, pressure, rainfall, wind).
- `benchmark.sh` : Compiles `q8_mpi`, generates/caches small/medium/large/very_large datasets in `bench_data/`, times each at `P = 1, 2, 4, 8` (best of 5 runs via `time` + `mpirun`), and writes `results.csv`.
- `verify_correctness.sh` : Compiles both `q8_mpi` and `q8_seq`, runs 5 test cases (including an uneven/prime `N=99999` to stress-test load balancing) at `P = 1, 2, 4, 8`, and diffs each MPI output against the sequential oracle.
- `plot_results.py` : Reads a results CSV (e.g. `python3 plot_results.py results.csv`) and writes `speedup_q8.png`, `runtime_table.csv`, `speedup_table.csv`.
- `run_q8.sh` : SLURM batch script — loads the MPI module, runs `verify_correctness.sh`, then runs `benchmark.sh`. (It does **not** call `plot_results.py` — run that manually after the job finishes, see below.)
- `results.csv`, `runtime_table.csv`, `speedup_table.csv` : Benchmark output data.
- `speedup_q8.png` : Generated speedup plot.

## How to execute on the cluster
1. Upload folder: `scp -r weather_analytics_mpi <username>@rce.iiit.ac.in:~/`
2. Log into cluster: `ssh <username>@rce.iiit.ac.in`
3. Enter folder: `cd weather_analytics_mpi`
4. Submit job: `sbatch run_q8.sh`

   `run_q8.sh` requests 2 nodes / 4 tasks per node (8 ranks total), loads `hpcx-2.7.0/hpcx-ompi`, then:
   - runs `verify_correctness.sh` (compiles both binaries, checks correctness at P=1,2,4,8 across 5 test cases)
   - runs `benchmark.sh` (compiles `q8_mpi` again, times P=1,2,4,8 on small/medium/large/very_large datasets, writes `results.csv`)
5. Check status: `squeue -u $USER`
6. Once complete, view log: `cat q8_benchmark_<JOB_ID>.log`
7. Generate the plots/tables (not run automatically by `run_q8.sh`):
   ```bash
   python3 plot_results.py results.csv
   ```

## Compiling manually (optional — verify/benchmark scripts already do this)
```bash
module load hpcx-2.7.0/hpcx-ompi
mpicxx -O2 -std=c++17 -o q8_mpi q8_mpi.cpp
g++    -O2 -std=c++17 -o q8_seq q8_seq.cpp
```

## Generating a dataset manually
```bash
python3 generate_dataset.py <N> <K> <S> <output_file> [seed]
# e.g.
python3 generate_dataset.py 500000 20 500 medium.txt 42
```

## Running verification / benchmarks manually
```bash
bash verify_correctness.sh   # builds both binaries, PASS/FAIL per test case × P
bash benchmark.sh            # builds q8_mpi, writes results.csv
python3 plot_results.py results.csv
```

## Download results locally
Log out of the cluster, then run locally:
```bash
scp <username>@rce.iiit.ac.in:~/weather_analytics_mpi/*.csv .
scp <username>@rce.iiit.ac.in:~/weather_analytics_mpi/*.png .
```
