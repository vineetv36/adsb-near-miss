#!/usr/bin/env python3
"""
Throughput benchmark for the near-miss detection pipeline.

Measures how many aircraft positions the detection pipeline can process per
second for different fleet sizes.  The benchmark mirrors the pandas-based
logic inside ``spark_streaming_job.process_batch()`` but runs entirely
in-process — no Kafka, Spark, or database required.

Usage
-----
    python benchmarks/throughput_test.py
    python benchmarks/throughput_test.py --fleet 50 100 200 500 --iterations 20
    make benchmark

Output
------
    Fleet  │ Median ms │  P95 ms │ Pairs │ Proximity │ Batches/s
    ───────┼───────────┼─────────┼───────┼───────────┼──────────
       50  │      12.4 │    14.1 │   247 │        0  │      80.6
      100  │      28.3 │    31.8 │   843 │        0  │      35.4
      200  │      89.1 │    97.2 │  2901 │        0  │      11.2
      500  │     541.2 │   572.0 │ 14203 │        0  │       1.8
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
import time
from typing import Any

import pandas as pd

# Ensure src/ is importable when run from the repo root or benchmarks/
_ROOT = os.path.dirname(os.path.dirname(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.processing.geohash_partitioner import encode as gh_encode, expand as gh_expand
from src.processing.proximity_detector import (
    classify_separation,
    haversine,
    normalize_altitude,
    vertical_separation,
)

# Detection thresholds (mirrors spark_streaming_job.py)
_PROX_NM = 8.0
_PROX_FT = 2_000.0
M_TO_FT  = 1.0 / 0.3048

# Simulate aircraft spread across the continental US
_US_LAT = (24.5, 49.0)
_US_LON = (-125.0, -66.5)
_ALT_M  = (6_000.0, 12_500.0)   # ≈ FL200–FL410
_SPD_MS = (180.0, 270.0)        # ≈ 350–525 kts


def _make_fleet(n: int, seed: int = 42) -> pd.DataFrame:
    """Return a DataFrame of *n* synthetic aircraft at random US positions."""
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append({
            "icao24":        f"{i:06x}",
            "latitude":      rng.uniform(*_US_LAT),
            "longitude":     rng.uniform(*_US_LON),
            "baro_altitude": rng.uniform(*_ALT_M),
            "geo_altitude":  rng.uniform(*_ALT_M),
            "velocity":      rng.uniform(*_SPD_MS),
            "true_track":    rng.uniform(0.0, 360.0),
        })
    return pd.DataFrame(rows)


def run_batch(pdf: pd.DataFrame) -> dict[str, Any]:
    """
    Execute one simulated 10-second micro-batch.

    Returns a dict with counts for pairs_checked, proximity_candidates, and
    classified events (by severity).
    """
    # ── Geohash at precision 4, expand to 3×3 neighbourhood ──────────────────
    pdf = pdf.copy()
    pdf["cells"] = pdf.apply(
        lambda r: gh_expand(gh_encode(r["latitude"], r["longitude"], 4)), axis=1
    )

    # ── Explode → self-join on cell ───────────────────────────────────────────
    exploded = (
        pdf.explode("cells")
           .rename(columns={"cells": "cell"})
           .reset_index(drop=True)
    )
    left  = exploded.add_suffix("_a").rename(columns={"cell_a": "cell"})
    right = exploded.add_suffix("_b").rename(columns={"cell_b": "cell"})
    pairs = left.merge(right, on="cell")

    # Remove self-pairs and symmetric duplicates
    pairs = pairs[pairs["icao24_a"] < pairs["icao24_b"]]
    pairs = pairs.drop_duplicates(subset=["icao24_a", "icao24_b"]).reset_index(drop=True)

    n_pairs = len(pairs)
    if pairs.empty:
        return {"pairs": 0, "candidates": 0, "events": 0}

    # ── Haversine horizontal separation ──────────────────────────────────────
    pairs["horiz_nm"] = pairs.apply(
        lambda r: haversine(
            r["latitude_a"], r["longitude_a"],
            r["latitude_b"], r["longitude_b"],
        ),
        axis=1,
    )

    # ── Altitude → feet ───────────────────────────────────────────────────────
    pairs["alt_a_m"] = pairs.apply(
        lambda r: normalize_altitude(r.get("baro_altitude_a"), r.get("geo_altitude_a")), axis=1
    )
    pairs["alt_b_m"] = pairs.apply(
        lambda r: normalize_altitude(r.get("baro_altitude_b"), r.get("geo_altitude_b")), axis=1
    )
    pairs["alt_a_ft"] = pairs["alt_a_m"].apply(lambda v: v * M_TO_FT if v is not None else None)
    pairs["alt_b_ft"] = pairs["alt_b_m"].apply(lambda v: v * M_TO_FT if v is not None else None)
    pairs["vert_ft"]  = pairs.apply(
        lambda r: vertical_separation(r["alt_a_ft"], r["alt_b_ft"]), axis=1
    )

    # ── Proximity filter ──────────────────────────────────────────────────────
    prox_mask = (pairs["horiz_nm"] < _PROX_NM) & (
        pairs["vert_ft"].isna() | (pairs["vert_ft"] < _PROX_FT)
    )
    candidates = pairs[prox_mask]
    n_candidates = len(candidates)

    # ── Classify ──────────────────────────────────────────────────────────────
    n_events = 0
    if not candidates.empty:
        severities = candidates.apply(
            lambda r: classify_separation(r["horiz_nm"], r["vert_ft"]).value, axis=1
        )
        n_events = (severities != "NORMAL").sum()

    return {"pairs": n_pairs, "candidates": n_candidates, "events": n_events}


def benchmark(fleet_sizes: list[int], iterations: int = 15) -> list[dict]:
    results = []

    for n in fleet_sizes:
        fleet = _make_fleet(n)

        # Warm-up — JIT caches, import overhead, etc.
        for _ in range(3):
            run_batch(fleet)

        times: list[float] = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            stats = run_batch(fleet)
            times.append(time.perf_counter() - t0)

        median_ms = statistics.median(times) * 1_000
        p95_ms    = sorted(times)[int(0.95 * len(times))] * 1_000
        results.append({
            "fleet":     n,
            "median_ms": median_ms,
            "p95_ms":    p95_ms,
            "pairs":     stats["pairs"],
            "proximity": stats["candidates"],
            "batches_s": 1_000 / median_ms,
        })

    return results


def _print_results(rows: list[dict]) -> None:
    header = f"{'Fleet':>6}  │ {'Median ms':>9} │ {'P95 ms':>7} │ {'Pairs':>6} │ {'Proximity':>9} │ {'Batches/s':>9}"
    sep    = "─" * 7 + "┼" + "─" * 11 + "┼" + "─" * 9 + "┼" + "─" * 8 + "┼" + "─" * 11 + "┼" + "─" * 11
    print()
    print(header)
    print(sep)
    for r in rows:
        print(
            f"{r['fleet']:>6}  │ {r['median_ms']:>9.1f} │ {r['p95_ms']:>7.1f} │"
            f" {r['pairs']:>6} │ {r['proximity']:>9} │ {r['batches_s']:>9.1f}"
        )
    print()

    if rows:
        best = rows[0]
        worst = rows[-1]
        print(f"  At {best['fleet']} aircraft: {best['batches_s']:.0f} batches/s  "
              f"({best['median_ms']:.1f} ms/batch)")
        print(f"  At {worst['fleet']} aircraft: {worst['batches_s']:.1f} batches/s  "
              f"({worst['median_ms']:.1f} ms/batch)")
        print()
        if worst["pairs"] > 0:
            scale = worst["fleet"] / best["fleet"]
            pair_scale = worst["pairs"] / best["pairs"]
            print(f"  Fleet ×{scale:.0f} → candidate pairs ×{pair_scale:.1f}  "
                  f"(quadratic growth expected for dense airspace)")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Throughput benchmark for the ADS-B near-miss detection pipeline"
    )
    parser.add_argument(
        "--fleet",
        nargs="+",
        type=int,
        default=[50, 100, 200, 500],
        metavar="N",
        help="Fleet sizes to benchmark (default: 50 100 200 500)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=15,
        metavar="N",
        help="Number of timed iterations per fleet size (default: 15)",
    )
    args = parser.parse_args()

    print(f"ADS-B Near-Miss Detection — Throughput Benchmark")
    print(f"  Fleet sizes : {args.fleet}")
    print(f"  Iterations  : {args.iterations} per size (+ 3 warm-up)")
    print(f"  Batch size  : 10 s (mirrors Spark micro-batch interval)")
    print(f"  Geohash     : precision 4 + 3×3 expansion  (≈60 km coverage)")

    results = benchmark(args.fleet, args.iterations)
    _print_results(results)


if __name__ == "__main__":
    main()
