import pandas as pd
import matplotlib.pyplot as plt
import sys

if len(sys.argv) < 2:
    sys.exit(1)

df = pd.read_csv(sys.argv[1])
runtime = df.pivot(index='size_label', columns='P', values='time_seconds')

speedup = runtime.copy()
efficiency = runtime.copy()
for p in [1, 2, 4, 8]:
    speedup[p] = speedup[1] / runtime[p]
    efficiency[p] = speedup[p] / p

runtime = runtime.reindex(['small', 'medium', 'large', 'very_large'])
speedup = speedup.reindex(['small', 'medium', 'large', 'very_large'])
efficiency = efficiency.reindex(['small', 'medium', 'large', 'very_large'])

# Plot Speedup
plt.figure(figsize=(8, 5))
for size in speedup.index:
    plt.plot([1, 2, 4, 8], speedup.loc[size], marker='o', label=size)
plt.plot([1, 2, 4, 8], [1, 2, 4, 8], 'k--', label='Ideal')
plt.title("Speedup vs Number of Processes (Q8 Analytics)")
plt.xlabel("Processes (P)")
plt.ylabel("Speedup S(P)")
plt.legend()
plt.grid(True)
plt.savefig("speedup_q8.png")

runtime.to_csv("runtime_table.csv")
speedup.to_csv("speedup_table.csv")