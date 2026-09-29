"""
hw3_grpc/tests/test_correctness.py — End-to-end correctness tests: HW3 vs HW2.

STATUS: PLACEHOLDER — tests will be implemented in the next iteration.

Future test coverage:
    - Generate a reproducible dataset using generate_dataset.py (same seed as HW2).
    - Run the HW2 sequential oracle (q8_seq) on the dataset and capture output.
    - Run the full HW3 pipeline (streaming client → coordinator → workers)
      on the SAME dataset.
    - Compare HW3 final analytics against HW2 sequential output field-by-field.
    - Test with multiple worker counts (1, 2, 4, 8).
    - Test with multiple dataset sizes.
    - Test with prime N to stress-test uneven distribution.

Correctness criteria:
    HW3 output must match HW2 sequential output EXACTLY for the same input,
    after all records have been ingested (end-of-stream).

Tie-breaking rules from HW2 must be preserved exactly:
    - Hottest/coldest: tie on temperature → lowest timestamp → lowest station_id
    - Busiest interval: tie → lowest interval index
    - Top stations: tie on count → lowest station_id
"""

# TODO (implementation iteration): implement end-to-end correctness test cases
