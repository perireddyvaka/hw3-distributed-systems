#!/bin/bash
# hw3_grpc/scripts/run_correctness.sh
#
# STATUS: PLACEHOLDER — implement in the next iteration.
#
# Future responsibility:
#   End-to-end correctness test comparing HW3 output to HW2 sequential oracle.
#
#   Approximate sequence:
#     1. Generate a reproducible dataset (fixed seed).
#     2. Run HW2 sequential oracle (hw2_mpi/src/q8_seq) to get reference output.
#     3. Start HW3 system (coordinator + workers).
#     4. Stream the dataset through HW3 pipeline.
#     5. Query final analytics from coordinator.
#     6. Compare HW3 output to reference output (field-by-field diff).
#     7. Report PASS / FAIL per test case.
#
#   Test cases should include multiple N, K, S, seed combinations
#   and multiple worker counts (1, 2, 4, 8).

echo "[run_correctness.sh] PLACEHOLDER — not yet implemented." >&2
exit 1
