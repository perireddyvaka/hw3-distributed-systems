# hw3_grpc/dataset/README.md

# HW3 Dataset

## Status
**Placeholder** — dataset generation will be implemented in the next iteration.

## Dataset Format
HW3 uses the **exact same** dataset format as HW2:

```
N K S
timestamp station_id temperature humidity pressure rainfall wind_speed
...
```

| Field         | Type   | Description                                  |
|---------------|--------|----------------------------------------------|
| N             | int    | Total number of records                      |
| K             | int    | Number of top stations to report             |
| S             | int    | Number of stations (station IDs: 0 to S-1)   |
| timestamp     | int    | Unix epoch seconds                           |
| station_id    | int    | Integer in [0, S-1]                          |
| temperature   | float  | Degrees Celsius                              |
| humidity      | float  | Percent [10.0, 100.0]                        |
| pressure      | float  | hPa [900.0, 1100.0]                          |
| rainfall      | float  | mm [0.0, 100.0]                              |
| wind_speed    | float  | km/h [0.0, 150.0]                            |

## Why the same format?
HW3 correctness is validated against the HW2 sequential reference (`q8_seq.cpp`).
Using the same dataset format ensures that both systems can be run on identical
input files and their outputs compared directly.

## Usage (future)
```bash
python hw3_grpc/dataset/generate_dataset.py <N> <K> <S> <output_file> [seed]
```

Generated datasets should be placed under `hw3_grpc/data/generated/` or
`comparison/datasets/` for cross-system comparison experiments.
