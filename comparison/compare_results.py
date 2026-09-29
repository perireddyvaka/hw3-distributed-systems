"""
comparison/compare_results.py — Correctness comparison: HW2 vs HW3.

STATUS: PLACEHOLDER — implementation deferred until HW3 is implemented.

Future responsibility:
    Compare analytics output files from:
        - HW2 sequential (reference/oracle)
        - HW2 MPI (P=1, 2, 4, 8)
        - HW3 gRPC (W=1, 2, 4, 8 workers)

    All systems run on the same datasets from comparison/datasets/.

    Correctness hierarchy:
        HW2 Sequential  →  (reference)
        HW2 MPI         →  must match HW2 Sequential exactly
        HW3 gRPC        →  must match HW2 Sequential exactly

    Output:
        - comparison/results/correctness/ : per-test-case diff files
        - comparison/results/comparison_summary.csv : PASS/FAIL table

Usage (future):
    python comparison/compare_results.py --dataset comparison/datasets/medium.txt
"""

# TODO (implementation iteration): implement field-by-field comparison logic
