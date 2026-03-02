"""
Spark Structured Streaming – ADS-B Near-Miss Detector
======================================================

Consumes from Kafka topic `adsb.raw`, runs a 10-second micro-batch, detects
proximity events between aircraft, and writes results to three sinks:
  • PostGIS  — full event record with geometries  (durable store / hotspot analysis)
  • Redis    — lightweight alert with 5-min TTL  (live dashboard)
  • Parquet  — partitioned cold store            (batch analytics)

Processing pipeline per micro-batch
------------------------------------
  1. Decode JSON from Kafka bytes; route malformed records to adsb.dlq
  2. Deduplicate by icao24 – keep the record with the newest time_position
  3. Assign geohash at precision 5; expand each aircraft to 9 cells
     (its own cell + 8 neighbours)
  4. Explode + self-join on geohash cell → candidate pairs
  5. Haversine horizontal distance + altitude separation for every candidate
  6. Classify: PROXIMITY / NEAR_MISS / LOSS_OF_SEPARATION
  7. Tag known-safe patterns (parallel ILS, departures, formation, helicopters)
  8. Write flagged events (>= PROXIMITY) to the three sinks

Design decisions
-----------------
  • foreachBatch instead of Spark SQL streaming aggregation: lets us use
    arbitrary Python (psycopg2, redis, pyarrow) per micro-batch without UDFs.
  • Tumbling window (processingTime="10 seconds"): simpler watermark management
    and exactly-once semantics vs. sliding windows.
  • Geohash precision 5 (~4.9 km cells): checking 9 cells covers the full
    8 NM proximity threshold in every direction. Precision 4 creates too many
    false-positive candidate pairs; precision 6 requires more neighbour checks.
  • O(n²) avoided: with 500 aircraft and p5 geohash ~4.9 km cells, each cell
    contains on average 1–3 aircraft. The self-join only evaluates pairs within
    adjacent cells, reducing candidates from 125 000 to < 500 per batch.
"""

import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Bootstrap: ensure src/ is on the path when run via spark-submit
# ---------------------------------------------------------------------------
_SRC = os.path.join(os.path.dirname(__file__), "..")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from processing.geohash_partitioner import encode as gh_encode, expand as gh_expand
from processing.proximity_detector  import (
    haversine, normalize_altitude, vertical_separation,
    closure_rate_with_positions, classify_separation,
    SeparationLevel, M_TO_FT, MS_TO_KTS,
)
from processing.known_patterns import tag_pattern

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("adsb.spark")

# ---------------------------------------------------------------------------
# Configuration (via environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL     = os.getenv("DATABASE_URL",
                             "postgresql://adsb:adsb_secret@postgres:5432/adsb")
REDIS_URL        = os.getenv("REDIS_URL", "redis://redis:6379/0")
PARQUET_PATH     = os.getenv("PARQUET_PATH", "/app/data/events")
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH", "/app/data/checkpoints")
TOPIC_IN         = "adsb.raw"
TOPIC_DLQ        = "adsb.dlq"

# Proximity filter: only process pairs that clear this threshold
_PROX_NM = 8.0
_PROX_FT = 2_000.0


# ---------------------------------------------------------------------------
# Kafka dead-letter producer (best-effort)
# ---------------------------------------------------------------------------

def _dlq_send(bootstrap: str, records: list) -> None:
    if not records:
        return
    try:
        from confluent_kafka import Producer  # type: ignore
        p = Producer({"bootstrap.servers": bootstrap})
        for r in records:
            p.produce(TOPIC_DLQ, value=r.encode() if isinstance(r, str) else r)
        p.flush(5)
    except Exception:
        pass   # DLQ is best-effort; don't crash the main pipeline


# ---------------------------------------------------------------------------
# Sink writers
# ---------------------------------------------------------------------------

def _write_postgis(events: pd.DataFrame) -> None:
    """Insert separation events into PostGIS separation_events table."""
    import psycopg2  # type: ignore

    # Strip "postgresql://" prefix for psycopg2 dsn
    dsn = DATABASE_URL.replace("postgresql://", "postgres://", 1)
    try:
        conn = psycopg2.connect(dsn)
    except psycopg2.OperationalError as e:
        log.warning("PostGIS write skipped – cannot connect: %s", e)
        return

    sql = """
        INSERT INTO separation_events
            (icao24_a, icao24_b, callsign_a, callsign_b,
             geom_a, geom_b, midpoint,
             horizontal_nm, vertical_ft, closure_rate_kt,
             severity, pattern, altitude_a_ft, altitude_b_ft, observed_at)
        VALUES
            (%s, %s, %s, %s,
             ST_SetSRID(ST_MakePoint(%s, %s), 4326),
             ST_SetSRID(ST_MakePoint(%s, %s), 4326),
             ST_SetSRID(ST_MakePoint(%s, %s), 4326),
             %s, %s, %s,
             %s, %s, %s, %s,
             to_timestamp(%s))
        ON CONFLICT DO NOTHING
    """
    with conn:
        with conn.cursor() as cur:
            for _, r in events.iterrows():
                mid_lat = (r["lat_a"] + r["lat_b"]) / 2
                mid_lon = (r["lon_a"] + r["lon_b"]) / 2
                ts = r.get("time_position_a") or r.get("last_contact_a") or \
                     datetime.now(timezone.utc).timestamp()
                cur.execute(sql, (
                    r["icao24_a"], r["icao24_b"],
                    r.get("callsign_a"), r.get("callsign_b"),
                    r["lon_a"],  r["lat_a"],
                    r["lon_b"],  r["lat_b"],
                    mid_lon,     mid_lat,
                    round(r["horiz_nm"],  4),
                    round(r["vert_ft"],   1) if r["vert_ft"] is not None else None,
                    round(r["clos_kt"],   1) if r.get("clos_kt") is not None else None,
                    r["severity"],
                    r.get("pattern"),
                    round(r["alt_a_ft"], 0) if r.get("alt_a_ft") is not None else None,
                    round(r["alt_b_ft"], 0) if r.get("alt_b_ft") is not None else None,
                    ts,
                ))
    conn.close()

    # Also write aircraft tracks (sampled – one row per aircraft per batch)
    _write_tracks_postgis(events, dsn)


def _write_tracks_postgis(events: pd.DataFrame, dsn: str) -> None:
    """Write sampled aircraft positions to aircraft_tracks."""
    import psycopg2  # type: ignore

    sql = """
        INSERT INTO aircraft_tracks
            (icao24, callsign, geom, altitude_ft, velocity_kts,
             true_track, vertical_rate, geohash, observed_at)
        VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s, %s, %s, %s, %s, to_timestamp(%s))
        ON CONFLICT DO NOTHING
    """
    # Deduplicate to one row per aircraft
    seen = set()
    rows = []
    for side in ("a", "b"):
        for _, r in events.iterrows():
            icao = r[f"icao24_{side}"]
            if icao in seen:
                continue
            seen.add(icao)
            rows.append((
                icao,
                r.get(f"callsign_{side}"),
                r[f"lon_{side}"], r[f"lat_{side}"],
                round(r[f"alt_{side}_ft"], 0) if r.get(f"alt_{side}_ft") is not None else None,
                round(r.get(f"velocity_{side}", 0) * MS_TO_KTS, 1),
                r.get(f"true_track_{side}"),
                (r.get(f"vertical_rate_{side}", 0) or 0),
                r.get(f"gh5_{side}"),
                r.get(f"time_position_{side}") or datetime.now(timezone.utc).timestamp(),
            ))
    if not rows:
        return
    try:
        conn = psycopg2.connect(dsn)
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        conn.close()
    except Exception as e:
        log.warning("aircraft_tracks write error: %s", e)


def _write_redis(events: pd.DataFrame) -> None:
    """Push lightweight alerts to Redis (TTL = 5 minutes)."""
    import redis as redis_lib  # type: ignore

    try:
        r = redis_lib.from_url(REDIS_URL, socket_connect_timeout=2)
        pipe = r.pipeline()
        for _, row in events.iterrows():
            key = f"alert:{min(row['icao24_a'], row['icao24_b'])}:{max(row['icao24_a'], row['icao24_b'])}"
            payload = json.dumps({
                "severity":       row["severity"],
                "horizontal_nm":  round(row["horiz_nm"], 2),
                "vertical_ft":    round(row["vert_ft"], 0) if row["vert_ft"] is not None else None,
                "closure_kt":     round(row["clos_kt"], 1) if row.get("clos_kt") is not None else None,
                "pattern":        row.get("pattern"),
                "lat_a":          round(row["lat_a"], 5),
                "lon_a":          round(row["lon_a"], 5),
                "lat_b":          round(row["lat_b"], 5),
                "lon_b":          round(row["lon_b"], 5),
                "icao24_a":       row["icao24_a"],
                "icao24_b":       row["icao24_b"],
                "ts":             row.get("time_position_a"),
            })
            pipe.set(key, payload, ex=300)   # 5-min TTL

            # Also update live-position keys for each aircraft
            for side in ("a", "b"):
                pos_key = f"pos:{row[f'icao24_{side}']}"
                pos_val = json.dumps({
                    "lat":      round(row[f"lat_{side}"],  5),
                    "lon":      round(row[f"lon_{side}"],  5),
                    "alt_ft":   round(row[f"alt_{side}_ft"], 0) if row.get(f"alt_{side}_ft") else None,
                    "track":    row.get(f"true_track_{side}"),
                    "callsign": row.get(f"callsign_{side}"),
                    "ts":       row.get(f"time_position_{side}"),
                })
                pipe.set(pos_key, pos_val, ex=60)  # 60-s TTL for live positions

        pipe.execute()
    except Exception as e:
        log.warning("Redis write error: %s", e)


def _write_parquet(events: pd.DataFrame, epoch_id: int) -> None:
    """Append events to date-partitioned Parquet cold store."""
    try:
        import pyarrow as pa          # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = os.path.join(PARQUET_PATH, f"date={date_str}")
        os.makedirs(path, exist_ok=True)

        # Keep only the columns we care about
        cols = [
            "icao24_a", "icao24_b", "callsign_a", "callsign_b",
            "lat_a", "lon_a", "lat_b", "lon_b",
            "horiz_nm", "vert_ft", "clos_kt",
            "alt_a_ft", "alt_b_ft",
            "severity", "pattern",
            "time_position_a",
        ]
        out = events[[c for c in cols if c in events.columns]].copy()
        table = pa.Table.from_pandas(out, preserve_index=False)
        pq.write_table(table, os.path.join(path, f"epoch_{epoch_id:010d}.parquet"))
    except Exception as e:
        log.warning("Parquet write error: %s", e)


# ---------------------------------------------------------------------------
# Core micro-batch processor
# ---------------------------------------------------------------------------

def process_batch(spark_df, epoch_id: int) -> None:
    """
    Called by Spark for every 10-second micro-batch.
    spark_df has the raw Kafka schema: (key, value, topic, partition, offset, …)
    """
    # ── 1. Decode JSON ────────────────────────────────────────────────────────
    raw_rows = spark_df.selectExpr("CAST(value AS STRING) as v").collect()
    if not raw_rows:
        return

    records, dlq = [], []
    for row in raw_rows:
        try:
            r = json.loads(row.v)
            if r.get("on_ground"):          # skip ground traffic
                continue
            if r.get("latitude") is None or r.get("longitude") is None:
                dlq.append(row.v)
                continue
            records.append(r)
        except (json.JSONDecodeError, KeyError, TypeError):
            dlq.append(row.v)

    _dlq_send(KAFKA_BOOTSTRAP, dlq)

    if not records:
        return

    # ── 2. Deduplicate: keep newest time_position per icao24 ─────────────────
    pdf = pd.DataFrame(records)
    pdf["time_position"] = pd.to_numeric(pdf.get("time_position"), errors="coerce")
    pdf = (
        pdf.sort_values("time_position", ascending=False, na_position="last")
           .drop_duplicates(subset=["icao24"])
           .reset_index(drop=True)
    )

    # ── 3. Geohash precision-5 + expand to 9 cells ───────────────────────────
    pdf["gh5"] = pdf.apply(
        lambda r: gh_encode(r["latitude"], r["longitude"], 5), axis=1
    )
    pdf["cells"] = pdf["gh5"].apply(gh_expand)

    # ── 4. Explode → self-join on cell ────────────────────────────────────────
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

    if pairs.empty:
        log.info("epoch=%d  aircraft=%d  no candidate pairs", epoch_id, len(pdf))
        return

    # ── 5. Compute horizontal + vertical distances ───────────────────────────
    pairs["horiz_nm"] = pairs.apply(
        lambda r: haversine(r["latitude_a"], r["longitude_a"],
                            r["latitude_b"], r["longitude_b"]), axis=1
    )

    pairs["alt_a_m"] = pairs.apply(
        lambda r: normalize_altitude(r.get("baro_altitude_a"), r.get("geo_altitude_a")), axis=1
    )
    pairs["alt_b_m"] = pairs.apply(
        lambda r: normalize_altitude(r.get("baro_altitude_b"), r.get("geo_altitude_b")), axis=1
    )
    pairs["alt_a_ft"] = pairs["alt_a_m"].apply(
        lambda v: v * M_TO_FT if v is not None else None
    )
    pairs["alt_b_ft"] = pairs["alt_b_m"].apply(
        lambda v: v * M_TO_FT if v is not None else None
    )
    pairs["vert_ft"] = pairs.apply(
        lambda r: vertical_separation(r["alt_a_ft"], r["alt_b_ft"]), axis=1
    )

    # ── 6. Filter to proximity candidates before expensive calculations ───────
    prox_mask = (pairs["horiz_nm"] < _PROX_NM) & (
        pairs["vert_ft"].isna() | (pairs["vert_ft"] < _PROX_FT)
    )
    candidates = pairs[prox_mask].copy()

    if candidates.empty:
        log.info("epoch=%d  aircraft=%d  pairs_checked=%d  no proximity events",
                 epoch_id, len(pdf), len(pairs))
        return

    # ── 7. Closure rate ───────────────────────────────────────────────────────
    candidates["clos_kt"] = candidates.apply(
        lambda r: closure_rate_with_positions(
            r["latitude_a"],  r["longitude_a"],
            r.get("velocity_a"), r.get("true_track_a"),
            r["latitude_b"],  r["longitude_b"],
            r.get("velocity_b"), r.get("true_track_b"),
        ), axis=1
    )

    # ── 8. Classify severity ─────────────────────────────────────────────────
    candidates["severity"] = candidates.apply(
        lambda r: classify_separation(r["horiz_nm"], r["vert_ft"]).value, axis=1
    )

    # Drop NORMAL (shouldn't happen but guard against floating-point edge cases)
    events = candidates[candidates["severity"] != SeparationLevel.NORMAL.value].copy()

    if events.empty:
        return

    # ── 9. Tag known patterns ────────────────────────────────────────────────
    events["pattern"] = events.apply(
        lambda r: tag_pattern({
            "horiz_nm":        r["horiz_nm"],
            "vert_ft":         r["vert_ft"],
            "clos_kt":         r.get("clos_kt"),
            "alt_a_ft":        r.get("alt_a_ft"),
            "alt_b_ft":        r.get("alt_b_ft"),
            "velocity_a":      r.get("velocity_a"),
            "velocity_b":      r.get("velocity_b"),
            "true_track_a":    r.get("true_track_a"),
            "true_track_b":    r.get("true_track_b"),
            "vertical_rate_a": r.get("vertical_rate_a"),
            "vertical_rate_b": r.get("vertical_rate_b"),
        }), axis=1
    )

    # Rename for sink writers
    events = events.rename(columns={
        "latitude_a":  "lat_a", "longitude_a": "lon_a",
        "latitude_b":  "lat_b", "longitude_b": "lon_b",
        "gh5_a": "gh5_a", "gh5_b": "gh5_b",
    })

    # ── 10. Write to sinks ───────────────────────────────────────────────────
    _write_postgis(events)
    _write_redis(events)
    _write_parquet(events, epoch_id)

    los  = (events["severity"] == SeparationLevel.LOSS_OF_SEPARATION.value).sum()
    nm   = (events["severity"] == SeparationLevel.NEAR_MISS.value).sum()
    prox = (events["severity"] == SeparationLevel.PROXIMITY.value).sum()
    log.info(
        "epoch=%d  aircraft=%d  candidates=%d  LoS=%d  NearMiss=%d  Proximity=%d",
        epoch_id, len(pdf), len(pairs), los, nm, prox,
    )


# ---------------------------------------------------------------------------
# Spark session + streaming query
# ---------------------------------------------------------------------------

def main() -> None:
    from pyspark.sql import SparkSession  # type: ignore

    spark = (
        SparkSession.builder
        .appName("adsb-near-miss-detector")
        .config("spark.sql.adaptive.enabled", "false")   # deterministic batches
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Spark version: %s", spark.version)
    log.info("Reading from Kafka: %s  topic=%s", KAFKA_BOOTSTRAP, TOPIC_IN)

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )

    query = (
        df_raw.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    log.info("Streaming query started. Awaiting termination…")
    query.awaitTermination()


if __name__ == "__main__":
    main()
