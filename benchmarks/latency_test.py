#!/usr/bin/env python3
"""
Latency micro-benchmark for individual detection functions.

Reports P50 / P95 / P99 latencies in microseconds for each function so hot
paths can be identified and optimised without running the full pipeline.

Usage
-----
    python benchmarks/latency_test.py
    python benchmarks/latency_test.py --calls 500000
    make benchmark

Output
------
    Function                        │   Calls │   P50 µs │   P95 µs │   P99 µs │   Max µs
    ────────────────────────────────┼─────────┼──────────┼──────────┼──────────┼─────────
    haversine                       │  200000 │     1.82 │     2.41 │     3.10 │    18.43
    classify_separation             │  200000 │     0.43 │     0.61 │     0.83 │     7.21
    normalize_altitude (both)       │  200000 │     0.31 │     0.44 │     0.62 │     5.87
    vertical_separation             │  200000 │     0.29 │     0.40 │     0.55 │     4.12
    closure_rate_with_positions     │  200000 │     1.94 │     2.63 │     3.45 │    21.08
    gh_encode (precision 4)         │  200000 │     4.71 │     6.18 │     8.02 │    43.60
    gh_expand (precision 4)         │  200000 │    46.22 │    58.93 │    74.51 │   312.77
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from collections.abc import Callable
from typing import Any

_ROOT = os.path.dirname(os.path.dirname(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.processing.geohash_partitioner import encode as gh_encode, expand as gh_expand
from src.processing.proximity_detector import (
    classify_separation,
    closure_rate_with_positions,
    haversine,
    normalize_altitude,
    vertical_separation,
)

_US_LAT = (24.5, 49.0)
_US_LON = (-125.0, -66.5)


def _percentile(sorted_times: list[float], p: float) -> float:
    idx = max(0, int(p * len(sorted_times)) - 1)
    return sorted_times[idx]


def _run(fn: Callable, args_list: list[tuple], calls: int) -> dict[str, float]:
    """Time *calls* invocations of fn, cycling through args_list."""
    n = len(args_list)
    times: list[float] = []
    # Warm-up
    for i in range(min(1_000, calls // 10)):
        fn(*args_list[i % n])
    # Timed loop — measure individually to get full distribution
    for i in range(calls):
        a = args_list[i % n]
        t0 = time.perf_counter()
        fn(*a)
        times.append(time.perf_counter() - t0)

    times_us = sorted(t * 1_000_000 for t in times)
    return {
        "calls": calls,
        "p50":   _percentile(times_us, 0.50),
        "p95":   _percentile(times_us, 0.95),
        "p99":   _percentile(times_us, 0.99),
        "max":   times_us[-1],
    }


def _make_args(rng: random.Random) -> dict[str, list[tuple]]:
    """Pre-generate random argument tuples so argument creation doesn't skew timing."""
    def lat(): return rng.uniform(*_US_LAT)
    def lon(): return rng.uniform(*_US_LON)
    def vel(): return rng.uniform(180.0, 270.0)
    def trk(): return rng.uniform(0.0, 360.0)
    def alt(): return rng.uniform(6_000.0, 12_500.0)

    N = 2_000  # pool size — large enough to avoid cache-hit bias
    return {
        "haversine": [
            (lat(), lon(), lat(), lon()) for _ in range(N)
        ],
        "classify_separation": [
            (rng.uniform(0.5, 10.0), rng.uniform(200.0, 3_000.0)) for _ in range(N)
        ],
        "normalize_altitude_both": [
            (alt(), alt()) for _ in range(N)
        ],
        "normalize_altitude_baro": [
            (alt(), None) for _ in range(N)
        ],
        "vertical_separation": [
            (rng.uniform(20_000.0, 40_000.0), rng.uniform(20_000.0, 40_000.0)) for _ in range(N)
        ],
        "closure_rate_with_positions": [
            (lat(), lon(), vel(), trk(), lat(), lon(), vel(), trk()) for _ in range(N)
        ],
        "gh_encode_p4": [
            (lat(), lon(), 4) for _ in range(N)
        ],
        "gh_expand_p4": [
            (gh_encode(lat(), lon(), 4),) for _ in range(N)
        ],
    }


def benchmark(calls: int, seed: int = 42) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    args = _make_args(rng)

    benchmarks = [
        ("haversine",                  haversine,                  args["haversine"]),
        ("classify_separation",        classify_separation,        args["classify_separation"]),
        ("normalize_altitude (both)",  normalize_altitude,         args["normalize_altitude_both"]),
        ("normalize_altitude (baro)",  normalize_altitude,         args["normalize_altitude_baro"]),
        ("vertical_separation",        vertical_separation,        args["vertical_separation"]),
        ("closure_rate_with_positions",closure_rate_with_positions,args["closure_rate_with_positions"]),
        ("gh_encode (precision 4)",    gh_encode,                  args["gh_encode_p4"]),
        ("gh_expand (precision 4)",    gh_expand,                  args["gh_expand_p4"]),
    ]

    results = []
    for name, fn, arg_list in benchmarks:
        stats = _run(fn, arg_list, calls)
        results.append({"name": name, **stats})

    return results


def _print_results(rows: list[dict[str, Any]]) -> None:
    name_w = max(len(r["name"]) for r in rows) + 2
    header = (
        f"{'Function':<{name_w}}│ {'Calls':>7} │ {'P50 µs':>8} │"
        f" {'P95 µs':>8} │ {'P99 µs':>8} │ {'Max µs':>8}"
    )
    sep = "─" * name_w + "┼" + "─" * 9 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 9
    print()
    print(header)
    print(sep)
    for r in rows:
        print(
            f"{r['name']:<{name_w}}│ {r['calls']:>7,} │ {r['p50']:>8.2f} │"
            f" {r['p95']:>8.2f} │ {r['p99']:>8.2f} │ {r['max']:>8.2f}"
        )
    print()

    # Highlight slowest function
    slowest = max(rows, key=lambda r: r["p99"])
    print(f"  Slowest at P99: {slowest['name']} — {slowest['p99']:.2f} µs")

    # Throughput of the tightest inner loop
    hav = next(r for r in rows if r["name"] == "haversine")
    if hav["p50"] > 0:
        pairs_per_sec = 1_000_000 / hav["p50"]
        print(f"  haversine throughput: {pairs_per_sec:,.0f} pair evaluations/s (P50)")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Latency micro-benchmark for ADS-B detection functions"
    )
    parser.add_argument(
        "--calls",
        type=int,
        default=200_000,
        metavar="N",
        help="Number of timed calls per function (default: 200 000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible results (default: 42)",
    )
    args = parser.parse_args()

    print("ADS-B Near-Miss Detection — Latency Micro-benchmark")
    print(f"  Calls per function : {args.calls:,}")
    print(f"  Argument pool size : 2 000 (pre-generated, excluded from timing)")

    results = benchmark(args.calls, args.seed)
    _print_results(results)


if __name__ == "__main__":
    main()
