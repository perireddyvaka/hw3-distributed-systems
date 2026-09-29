#!/usr/bin/env python3
"""
comparison/compare_results.py — Correctness comparison: HW2 vs HW3.

Parses output from:
    - HW2 sequential oracle (q8_seq)
    - HW2 MPI implementation (q8_mpi)
    - HW3 gRPC real-time analytics system (query_client / dashboard output)

Validates all 14 global weather metrics, extreme temperature events,
hottest/coldest measurements, busiest interval, and top-K station rankings
within floating-point tolerance (default 1e-4).
"""

import argparse
import csv
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

TOLERANCE = 1e-4

FLOAT_KEYS = [
    "AVERAGE_TEMPERATURE",
    "MIN_TEMPERATURE",
    "MAX_TEMPERATURE",
    "AVERAGE_HUMIDITY",
    "MIN_HUMIDITY",
    "MAX_HUMIDITY",
    "AVERAGE_PRESSURE",
    "MIN_PRESSURE",
    "MAX_PRESSURE",
    "TOTAL_RAINFALL",
    "MAX_RAINFALL",
    "AVERAGE_WIND_SPEED",
    "MAX_WIND_SPEED",
]

INT_KEYS = [
    "TOTAL_MEASUREMENTS",
    "EXTREME_TEMPERATURE_EVENTS",
]


def parse_weather_output(text: str) -> Dict[str, Any]:
    """Parse weather analytics report into a typed dictionary."""
    data: Dict[str, Any] = {}
    lines = text.strip().splitlines()
    in_top_stations = False
    top_stations: List[Dict[str, Any]] = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line == "TOP_STATIONS":
            in_top_stations = True
            continue
        if in_top_stations:
            parts = line.split()
            if len(parts) >= 4:
                top_stations.append({
                    "station_id": int(parts[0]),
                    "count": int(parts[1]),
                    "avg_temperature": float(parts[2]),
                    "total_rainfall": float(parts[3]),
                })
            continue

        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        key, val = parts[0], parts[1]

        if key in INT_KEYS:
            data[key] = int(val)
        elif key in FLOAT_KEYS:
            data[key] = float(val)
        elif key == "HOTTEST_MEASUREMENT":
            p = val.split()
            data[key] = {"timestamp": int(p[0]), "station_id": int(p[1]), "temperature": float(p[2])}
        elif key == "COLDEST_MEASUREMENT":
            p = val.split()
            data[key] = {"timestamp": int(p[0]), "station_id": int(p[1]), "temperature": float(p[2])}
        elif key == "BUSIEST_INTERVAL":
            p = val.split()
            data[key] = {"interval": int(p[0]), "count": int(p[1])}

    data["TOP_STATIONS"] = top_stations
    return data


def compare_parsed_outputs(
    ref: Dict[str, Any],
    test: Dict[str, Any],
    tol: float = TOLERANCE,
) -> Tuple[bool, List[str], float]:
    """
    Compare two parsed weather analytics outputs.

    Returns:
        (passed, list_of_error_strings, max_absolute_float_diff)
    """
    errors: List[str] = []
    max_diff: float = 0.0

    # 1. Check integer metrics
    for k in INT_KEYS:
        if k in ref:
            if k not in test:
                errors.append(f"Missing key in test output: {k}")
            elif ref[k] != test[k]:
                errors.append(f"{k}: Expected {ref[k]}, got {test[k]}")

    # 2. Check float metrics
    for k in FLOAT_KEYS:
        if k in ref:
            if k not in test:
                errors.append(f"Missing key in test output: {k}")
            else:
                diff = abs(ref[k] - test[k])
                max_diff = max(max_diff, diff)
                if diff > tol:
                    errors.append(f"{k}: Expected {ref[k]:.6f}, got {test[k]:.6f} (diff={diff:.6e} > {tol})")

    # 3. Check hottest measurement
    if "HOTTEST_MEASUREMENT" in ref:
        if "HOTTEST_MEASUREMENT" not in test:
            errors.append("Missing HOTTEST_MEASUREMENT in test output")
        else:
            rh, th = ref["HOTTEST_MEASUREMENT"], test["HOTTEST_MEASUREMENT"]
            if rh["station_id"] != th["station_id"] or rh["timestamp"] != th["timestamp"]:
                errors.append(
                    f"HOTTEST_MEASUREMENT record mismatch: ref=(ts={rh['timestamp']}, st={rh['station_id']}) "
                    f"vs test=(ts={th['timestamp']}, st={th['station_id']})"
                )
            diff = abs(rh["temperature"] - th["temperature"])
            max_diff = max(max_diff, diff)
            if diff > tol:
                errors.append(f"HOTTEST_MEASUREMENT temperature diff: {diff:.6e} > {tol}")

    # 4. Check coldest measurement
    if "COLDEST_MEASUREMENT" in ref:
        if "COLDEST_MEASUREMENT" not in test:
            errors.append("Missing COLDEST_MEASUREMENT in test output")
        else:
            rc, tc = ref["COLDEST_MEASUREMENT"], test["COLDEST_MEASUREMENT"]
            if rc["station_id"] != tc["station_id"] or rc["timestamp"] != tc["timestamp"]:
                errors.append(
                    f"COLDEST_MEASUREMENT record mismatch: ref=(ts={rc['timestamp']}, st={rc['station_id']}) "
                    f"vs test=(ts={tc['timestamp']}, st={tc['station_id']})"
                )
            diff = abs(rc["temperature"] - tc["temperature"])
            max_diff = max(max_diff, diff)
            if diff > tol:
                errors.append(f"COLDEST_MEASUREMENT temperature diff: {diff:.6e} > {tol}")

    # 5. Check busiest interval
    if "BUSIEST_INTERVAL" in ref:
        if "BUSIEST_INTERVAL" not in test:
            errors.append("Missing BUSIEST_INTERVAL in test output")
        else:
            rb, tb = ref["BUSIEST_INTERVAL"], test["BUSIEST_INTERVAL"]
            if rb["interval"] != tb["interval"] or rb["count"] != tb["count"]:
                errors.append(
                    f"BUSIEST_INTERVAL mismatch: ref=(interval={rb['interval']}, count={rb['count']}) "
                    f"vs test=(interval={tb['interval']}, count={tb['count']})"
                )

    # 6. Check top stations
    ref_top = ref.get("TOP_STATIONS", [])
    test_top = test.get("TOP_STATIONS", [])
    if len(ref_top) != len(test_top):
        errors.append(f"TOP_STATIONS count mismatch: ref={len(ref_top)}, test={len(test_top)}")
    else:
        for idx, (rs, ts) in enumerate(zip(ref_top, test_top)):
            if rs["station_id"] != ts["station_id"]:
                errors.append(
                    f"TOP_STATIONS[{idx}] station_id mismatch: ref={rs['station_id']}, test={ts['station_id']}"
                )
            if rs["count"] != ts["count"]:
                errors.append(f"TOP_STATIONS[{idx}] count mismatch: ref={rs['count']}, test={ts['count']}")
            d_temp = abs(rs["avg_temperature"] - ts["avg_temperature"])
            d_rain = abs(rs["total_rainfall"] - ts["total_rainfall"])
            max_diff = max(max_diff, d_temp, d_rain)
            if d_temp > tol:
                errors.append(f"TOP_STATIONS[{idx}] avg_temp diff: {d_temp:.6e} > {tol}")
            if d_rain > tol:
                errors.append(f"TOP_STATIONS[{idx}] rainfall diff: {d_rain:.6e} > {tol}")

    passed = len(errors) == 0
    return passed, errors, max_diff


def compare_files(
    ref_file: Path,
    test_file: Path,
    label: str = "",
    diff_file: Optional[Path] = None,
    tol: float = TOLERANCE,
) -> Tuple[bool, float]:
    """Compare two output files and optionally write diff report."""
    ref_text = ref_file.read_text(encoding="utf-8")
    test_text = test_file.read_text(encoding="utf-8")

    ref_data = parse_weather_output(ref_text)
    test_data = parse_weather_output(test_text)

    passed, errors, max_diff = compare_parsed_outputs(ref_data, test_data, tol=tol)

    print(f"[{'PASS' if passed else 'FAIL'}] {label or ref_file.name + ' vs ' + test_file.name}")
    print(f"       Max Absolute Float Difference: {max_diff:.6e} (tolerance: {tol})")

    if not passed:
        print(f"       Discrepancies found ({len(errors)}):")
        for err in errors[:10]:
            print(f"         - {err}")
        if len(errors) > 10:
            print(f"         ... and {len(errors) - 10} more.")

    if diff_file:
        diff_file.parent.mkdir(parents=True, exist_ok=True)
        with open(diff_file, "w", encoding="utf-8") as f:
            f.write(f"Comparison: {ref_file} vs {test_file}\n")
            f.write(f"Status: {'PASS' if passed else 'FAIL'}\n")
            f.write(f"Max Diff: {max_diff:.6e}\n\n")
            if errors:
                f.write("Errors:\n")
                for err in errors:
                    f.write(f"  {err}\n")
            else:
                f.write("All metrics matched within tolerance.\n")

    return passed, max_diff


def main() -> None:
    parser = argparse.ArgumentParser(description="Correctness comparison: HW2 vs HW3.")
    parser.add_argument("ref_file", type=Path, help="Reference oracle output file (e.g. HW2 seq)")
    parser.add_argument("test_file", type=Path, help="Test output file (e.g. HW2 MPI or HW3 gRPC)")
    parser.add_argument("--label", type=str, default="", help="Label for this test case")
    parser.add_argument("--diff-file", type=Path, default=None, help="Path to write detailed diff file")
    parser.add_argument("--summary-csv", type=Path, default=None, help="Append row to summary CSV")
    parser.add_argument("--tolerance", type=float, default=TOLERANCE, help="Floating-point tolerance")
    args = parser.parse_args()

    passed, max_diff = compare_files(
        args.ref_file,
        args.test_file,
        label=args.label,
        diff_file=args.diff_file,
        tol=args.tolerance,
    )

    if args.summary_csv:
        args.summary_csv.parent.mkdir(parents=True, exist_ok=True)
        file_exists = args.summary_csv.exists()
        with open(args.summary_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["label", "ref_file", "test_file", "status", "max_diff", "tolerance"])
            writer.writerow([
                args.label or args.test_file.stem,
                str(args.ref_file),
                str(args.test_file),
                "PASS" if passed else "FAIL",
                f"{max_diff:.6e}",
                args.tolerance,
            ])

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
