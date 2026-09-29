"""hw3_grpc/dataset/generate_dataset.py — HW3 dataset generator.

Produces reproducible weather datasets in the EXACT same format as HW2.
Compatible with hw2_mpi/dataset/generate_dataset.py (same random logic, same seed).

Format:
    Line 1:       N K S
    Lines 2..N+1: timestamp station_id temperature humidity pressure rainfall wind_speed

Usage:
    python hw3_grpc/dataset/generate_dataset.py \\
        --records 100000 --top-k 10 --stations 100 \\
        --output hw3_grpc/data/generated/data.txt [--seed 42]
"""

import argparse
import os
import random
import sys


def generate_dataset(
    n: int, k: int, s: int, filename: str, seed: int = 42
) -> None:
    """Generate a reproducible weather dataset.

    Parameters match the HW2 generator exactly for cross-system compatibility:
        n  = number of measurements
        k  = top-K stations parameter
        s  = number of stations (station IDs: 0 to s-1)
    """
    random.seed(seed)
    parent_dir = os.path.dirname(os.path.abspath(filename))
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(filename, "w") as f:
        f.write(f"{n} {k} {s}\n")
        for _ in range(n):
            ts = random.randint(1600000000, 1600008640)
            st = random.randint(0, s - 1)
            temp = round(random.uniform(-10.0, 50.0), 6)
            hum = round(random.uniform(10.0, 100.0), 6)
            pres = round(random.uniform(900.0, 1100.0), 6)
            rain = round(random.uniform(0.0, 100.0), 6)
            wind = round(random.uniform(0.0, 150.0), 6)
            f.write(
                f"{ts} {st} {temp:.6f} {hum:.6f} {pres:.6f} {rain:.6f} {wind:.6f}\n"
            )
    print(f"Generated {n} records → {filename}")


def main() -> None:
    if len(sys.argv) >= 5 and not sys.argv[1].startswith("-"):
        # Positional arguments: <N> <K> <S> <output_file> [seed]
        n = int(sys.argv[1])
        k = int(sys.argv[2])
        s = int(sys.argv[3])
        out_file = sys.argv[4]
        seed = int(sys.argv[5]) if len(sys.argv) > 5 else 42
        generate_dataset(n=n, k=k, s=s, filename=out_file, seed=seed)
        return

    parser = argparse.ArgumentParser(
        description="Generate HW3-compatible weather dataset (same format as HW2)."
    )
    parser.add_argument("--records", "-n", type=int, required=True, help="Number of measurements (N)")
    parser.add_argument("--top-k", "-k", type=int, default=10, help="Top-K stations parameter")
    parser.add_argument("--stations", "-s", type=int, required=True, help="Number of stations (S)")
    parser.add_argument("--output", "-o", required=True, help="Output file path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = parser.parse_args()

    generate_dataset(
        n=args.records,
        k=args.top_k,
        s=args.stations,
        filename=args.output,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
