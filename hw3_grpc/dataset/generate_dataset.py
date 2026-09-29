"""
hw3_grpc/dataset/generate_dataset.py — HW3-compatible dataset generator.

STATUS: PLACEHOLDER — implementation deferred to next iteration.

Future responsibility:
    Generate reproducible weather datasets in the EXACT same format as the
    HW2 generator (hw2_mpi/dataset/generate_dataset.py) so that correctness
    comparisons between HW2 and HW3 are meaningful.

Dataset format (unchanged from HW2):
    Line 1:      N K S
    Lines 2..N+1: timestamp station_id temperature humidity pressure rainfall wind_speed

    where:
        timestamp   : int   (Unix epoch seconds)
        station_id  : int   (0 to S-1)
        temperature : float (degrees Celsius)
        humidity    : float (percent)
        pressure    : float (hPa)
        rainfall    : float (mm)
        wind_speed  : float (km/h)

IMPORTANT:
    - Do NOT change the HW2 dataset format.
    - Do NOT duplicate or modify hw2_mpi/dataset/generate_dataset.py directly.
    - This module may simply import and re-export the HW2 generator function,
      or it may be an independent equivalent implementation.
    - The seed must be reproducible so that HW2 and HW3 use identical datasets.

Usage (future):
    python hw3_grpc/dataset/generate_dataset.py <N> <K> <S> <output_file> [seed]
"""

# TODO (implementation iteration): implement or import generate_dataset()
