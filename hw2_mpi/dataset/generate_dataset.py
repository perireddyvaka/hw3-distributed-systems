import sys
import random

def generate_dataset(N, K, S, filename, seed=42):
    random.seed(seed)
    with open(filename, 'w') as f:
        f.write(f"{N} {K} {S}\n")
        for _ in range(N):
            ts = random.randint(1600000000, 1600008640) # random day
            st = random.randint(0, S - 1)
            temp = round(random.uniform(-10.0, 50.0), 6)
            hum = round(random.uniform(10.0, 100.0), 6)
            pres = round(random.uniform(900.0, 1100.0), 6)
            rain = round(random.uniform(0.0, 100.0), 6)
            wind = round(random.uniform(0.0, 150.0), 6)
            f.write(f"{ts} {st} {temp:.6f} {hum:.6f} {pres:.6f} {rain:.6f} {wind:.6f}\n")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python generate_dataset.py <N> <K> <S> <output_file>")
        sys.exit(1)
    generate_dataset(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])