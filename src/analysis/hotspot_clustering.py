#!/usr/bin/env python3
"""
Hotspot clustering — HDBSCAN over historical near-miss event midpoints.

Usage (from repo root):
    python src/analysis/hotspot_clustering.py                # uses DATABASE_URL env var
    python src/analysis/hotspot_clustering.py --dry-run      # print clusters, no DB write
    python src/analysis/hotspot_clustering.py --min-cluster-size 3

Algorithm
---------
1. Query separation_events WHERE severity IN ('NEAR_MISS','LOSS_OF_SEPARATION')
   AND pattern IS NULL  (exclude known-safe events like parallel ILS approaches)
2. Convert midpoint lat/lon → radians  (required by HDBSCAN haversine metric)
3. Run HDBSCAN(min_cluster_size=5, min_samples=3, metric='haversine')
4. For each cluster (label ≥ 0):
   - Convex hull of member midpoints  (via Shapely)
   - Centroid, event count, temporal bounds
   - Severity-weighted score (LoS=2, NM=1)
   - Nearest major airport  (Haversine lookup against hardcoded list)
   - Approximate airspace class  (proximity-based heuristic)
5. DELETE + INSERT into hotspots table

Why HDBSCAN over DBSCAN?
  HDBSCAN handles varying-density clusters better — some hotspots are tight
  (airport TMA) while others are diffuse (en-route corridors). No global
  epsilon to tune.  See docs/ARCHITECTURE.md §5.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import numpy as np
import psycopg2
import psycopg2.extras

try:
    import hdbscan as hdbscan_lib
except ImportError:
    sys.exit("hdbscan not installed — run: pip install hdbscan")

try:
    from shapely.geometry import MultiPoint, Point
except ImportError:
    sys.exit("shapely not installed — run: pip install shapely")


# ── Constants ─────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://adsb:adsb_secret@localhost:5432/adsb"
)

EARTH_R_KM = 6_371.0

# Major US airports — ICAO → (lat, lon)
AIRPORTS: dict[str, tuple[float, float]] = {
    "KATL": (33.6407, -84.4277),
    "KLAX": (33.9425, -118.4081),
    "KORD": (41.9742, -87.9073),
    "KDFW": (32.8998, -97.0403),
    "KDEN": (39.8561, -104.6737),
    "KJFK": (40.6413, -73.7781),
    "KSFO": (37.6213, -122.3790),
    "KLAS": (36.0840, -115.1537),
    "KSEA": (47.4502, -122.3088),
    "KMIA": (25.7959, -80.2870),
    "KBOS": (42.3656, -71.0096),
    "KPHX": (33.4373, -112.0078),
    "KIAH": (29.9902, -95.3368),
    "KEWR": (40.6895, -74.1745),
    "KMSP": (44.8848, -93.2223),
    "KDTW": (42.2162, -83.3554),
    "KPHL": (39.8744, -75.2424),
    "KLGA": (40.7772, -73.8726),
    "KBWI": (39.1754, -76.6683),
    "KDCA": (38.8521, -77.0377),
    "KSLC": (40.7899, -111.9791),
    "KSAN": (32.7336, -117.1896),
    "KTPA": (27.9755, -82.5332),
    "KMDW": (41.7868, -87.7522),
    "KBNA": (36.1245, -86.6782),
    "KAUS": (30.1975, -97.6664),
    "KPDX": (45.5898, -122.5951),
    "KSTL": (38.7487, -90.3700),
    "KRDU": (35.8776, -78.7875),
    "KCLT": (35.2140, -80.9431),
}

# Class-B airports (busiest, most controlled)
CLASS_B_AIRPORTS = {
    "KATL", "KLAX", "KORD", "KDFW", "KDEN", "KJFK", "KSFO",
    "KLAS", "KSEA", "KMIA", "KBOS", "KPHX", "KIAH", "KEWR",
    "KMSP", "KDTW", "KPHL", "KLGA", "KBWI", "KDCA",
}


# ── Geometry helpers ──────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    φ1, λ1 = math.radians(lat1), math.radians(lon1)
    φ2, λ2 = math.radians(lat2), math.radians(lon2)
    dφ, dλ = φ2 - φ1, λ2 - λ1
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return 2 * EARTH_R_KM * math.asin(math.sqrt(max(0.0, a)))


def nearest_airport(lat: float, lon: float) -> str:
    best_code, best_dist = "UNKN", float("inf")
    for code, (alat, alon) in AIRPORTS.items():
        d = haversine_km(lat, lon, alat, alon)
        if d < best_dist:
            best_dist, best_code = d, code
    return best_code


def infer_airspace_class(lat: float, lon: float) -> str:
    """
    Heuristic airspace classification without FAA shapefiles.

    ≤ 10 km from a Class-B airport core  → 'B'
    ≤ 40 km from any major airport        → 'C'
    Otherwise                             → 'E'

    A proper implementation would use the FAA Airspace shapefile
    (scripts/download_faa_airspace.py) and do a true point-in-polygon test.
    """
    for code, (alat, alon) in AIRPORTS.items():
        d = haversine_km(lat, lon, alat, alon)
        if d <= 10 and code in CLASS_B_AIRPORTS:
            return "B"
        if d <= 40:
            return "C"
    return "E"


def polygon_wkt(lats: list[float], lons: list[float]) -> str:
    """
    Return a WKT polygon (convex hull of the cluster's midpoints).
    Falls back to a small circular buffer for degenerate cases.
    """
    pts = MultiPoint([(lon, lat) for lat, lon in zip(lats, lons)])
    hull = pts.convex_hull

    if hull.geom_type == "Point":
        # Single unique location — buffer ~0.3° (≈ 25 km) so the polygon is
        # visible at zoom 4 on the map.  A tight cluster of events all from the
        # same aircraft pair produces a degenerate single-point hull.
        hull = hull.buffer(0.3)
    elif hull.geom_type == "LineString":
        hull = hull.buffer(0.15)

    return hull.wkt


# ── Database helpers ──────────────────────────────────────────────────────────

def fetch_events(conn: Any) -> list[dict]:
    """
    Return near-miss / LoS events that are NOT tagged as known safe patterns.
    Only events with midpoint data (required for clustering) are included.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                ST_Y(midpoint)  AS lat,
                ST_X(midpoint)  AS lon,
                severity,
                pattern,
                observed_at
            FROM separation_events
            WHERE severity IN ('NEAR_MISS', 'LOSS_OF_SEPARATION')
              AND pattern IS NULL
              AND midpoint IS NOT NULL
            ORDER BY observed_at
            """
        )
        rows = cur.fetchall()
    logging.info("Fetched %d qualifying events for clustering", len(rows))
    return [dict(r) for r in rows]


# ── Clustering ────────────────────────────────────────────────────────────────

def run_hdbscan(
    events: list[dict],
    min_cluster_size: int = 3,
    min_samples: int = 2,
) -> np.ndarray:
    """
    Fit HDBSCAN on event midpoints (lat/lon in radians for haversine metric).
    Returns label array; −1 = noise (not assigned to any cluster).
    """
    coords_rad = np.radians([[e["lat"], e["lon"]] for e in events])

    clusterer = hdbscan_lib.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="haversine",
        cluster_selection_method="eom",   # excess-of-mass: stable, fewer micro-clusters
    )
    clusterer.fit(coords_rad)

    n_clusters = len(set(clusterer.labels_)) - (1 if -1 in clusterer.labels_ else 0)
    n_noise = int((clusterer.labels_ == -1).sum())
    logging.info(
        "HDBSCAN: %d clusters, %d noise points (%.1f%% classified)",
        n_clusters,
        n_noise,
        100 * (1 - n_noise / len(events)) if events else 0,
    )
    return clusterer.labels_


# ── Cluster record construction ───────────────────────────────────────────────

def build_cluster_records(
    events: list[dict], labels: np.ndarray
) -> list[dict]:
    """
    For each HDBSCAN cluster label ≥ 0, compute aggregate statistics and
    geometry.  Returns a list of dicts ready for insertion into hotspots.
    """
    buckets: dict[int, list[dict]] = defaultdict(list)
    for event, label in zip(events, labels):
        if label >= 0:
            buckets[int(label)].append(event)

    records = []
    for label, evs in sorted(buckets.items()):
        lats = [e["lat"] for e in evs]
        lons = [e["lon"] for e in evs]

        clat = sum(lats) / len(lats)
        clon = sum(lons) / len(lons)

        # Severity score: LoS=2, NM=1
        scores = [2 if e["severity"] == "LOSS_OF_SEPARATION" else 1 for e in evs]
        avg_severity = sum(scores) / len(scores)

        patterns = [e["pattern"] for e in evs if e["pattern"]]
        dominant_pattern = Counter(patterns).most_common(1)[0][0] if patterns else None

        times = [e["observed_at"] for e in evs]

        records.append(
            {
                "hull_wkt": polygon_wkt(lats, lons),
                "centroid_wkt": f"POINT({clon} {clat})",
                "event_count": len(evs),
                "avg_severity": round(avg_severity, 3),
                "dominant_pattern": dominant_pattern,
                "airspace_class": infer_airspace_class(clat, clon),
                "nearest_airport": nearest_airport(clat, clon),
                "first_seen": min(times),
                "last_seen": max(times),
            }
        )
        logging.debug(
            "  Cluster %d: %d events, airport=%s, class=%s, avg_sev=%.2f",
            label,
            len(evs),
            records[-1]["nearest_airport"],
            records[-1]["airspace_class"],
            avg_severity,
        )

    return records


# ── DB write ──────────────────────────────────────────────────────────────────

def write_hotspots(conn: Any, records: list[dict], dry_run: bool = False) -> None:
    """
    Replace all rows in the hotspots table with freshly computed clusters.
    Uses a DELETE + INSERT pattern (not UPSERT) because cluster IDs shift
    every run — the table is a derived/computed view of separation_events.
    """
    if dry_run:
        logging.info("[dry-run] Would write %d hotspot records:", len(records))
        for i, r in enumerate(records, 1):
            logging.info(
                "  #%d  events=%d  airport=%s  class=%s  sev=%.2f  "
                "first=%s  last=%s",
                i,
                r["event_count"],
                r["nearest_airport"],
                r["airspace_class"],
                r["avg_severity"],
                r["first_seen"],
                r["last_seen"],
            )
        return

    with conn.cursor() as cur:
        cur.execute("DELETE FROM hotspots")
        logging.info("Cleared existing hotspot rows")

        for r in records:
            cur.execute(
                """
                INSERT INTO hotspots (
                    geom, centroid, event_count, avg_severity,
                    dominant_pattern, airspace_class, nearest_airport,
                    first_seen, last_seen, computed_at
                )
                VALUES (
                    ST_GeomFromText(%s, 4326),
                    ST_GeomFromText(%s, 4326),
                    %s, %s, %s, %s, %s, %s, %s,
                    NOW()
                )
                """,
                (
                    r["hull_wkt"],
                    r["centroid_wkt"],
                    r["event_count"],
                    r["avg_severity"],
                    r["dominant_pattern"],
                    r["airspace_class"],
                    r["nearest_airport"],
                    r["first_seen"],
                    r["last_seen"],
                ),
            )

    conn.commit()
    logging.info("Wrote %d hotspot clusters to database", len(records))


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="HDBSCAN hotspot clustering for ADS-B near-miss events"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print clusters without writing to the database",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=3,
        metavar="N",
        help="HDBSCAN min_cluster_size (default: 3)",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=2,
        metavar="N",
        help="HDBSCAN min_samples (default: 2)",
    )
    parser.add_argument(
        "--db-url",
        default=DATABASE_URL,
        help="PostgreSQL connection URL (default: $DATABASE_URL)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = psycopg2.connect(args.db_url)
    try:
        events = fetch_events(conn)

        if len(events) < args.min_cluster_size:
            logging.warning(
                "Only %d qualifying events — need at least %d for clustering. "
                "Let the simulator run for a few more minutes: make simulate-small",
                len(events),
                args.min_cluster_size,
            )
            return

        labels = run_hdbscan(
            events,
            min_cluster_size=args.min_cluster_size,
            min_samples=args.min_samples,
        )
        records = build_cluster_records(events, labels)

        if not records:
            logging.warning("No clusters found. Lower --min-cluster-size or ingest more data.")
            return

        write_hotspots(conn, records, dry_run=args.dry_run)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
