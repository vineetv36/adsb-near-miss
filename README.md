# ADS-B Near-Miss Detector

Real-time airspace separation-event detector built on Kafka, Spark Structured
Streaming, PostGIS, and Redis — with a live deck.gl / MapLibre map.

The system ingests ADS-B aircraft state vectors (from OpenSky Network or a
built-in simulator), detects loss-of-separation, near-miss, and proximity
events using geohash-based candidate filtering and the ICAO en-route separation
standard, and surfaces results through a REST + WebSocket API and an
interactive map with hotspot-cluster overlays.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                                │
│                                                                              │
│  src/ingestion/adsb_simulator.py  ──┐   (synthetic flights, near-miss pairs) │
│  src/ingestion/opensky_producer.py ─┤   (live OpenSky REST → Kafka)          │
└─────────────────────────────────────┼────────────────────────────────────────┘
                                      │ JSON state-vectors
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  KAFKA                                                                       │
│  topic: adsb.raw  (6 partitions, retention 1 h)                             │
│  topic: adsb.dlq  (1 partition  — decode failures / bad records)            │
└───────────────────────────┬──────────────────────────────────────────────────┘
                            │ Spark Kafka source
                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SPARK STRUCTURED STREAMING  (10-second micro-batches, foreachBatch)        │
│                                                                              │
│  1. JSON decode + on-ground filter + dedup (newest ping per icao24)         │
│  2. Write ALL positions → Redis  aircraft:{icao24}  TTL 60 s                │
│  3. Geohash encode at precision 4 (≈ 20 km cells)                           │
│     + 3×3 neighbour expansion → 9-cell candidate set (≈ 60 km coverage)    │
│  4. Self-join on shared cell → candidate pairs                               │
│  5. Haversine horizontal distance + vertical separation (ft)                │
│  6. Proximity filter: horiz < 8 NM AND vert < 2 000 ft                     │
│  7. ICAO classification: Loss of Sep / Near-Miss / Proximity                │
│  8. Closure rate + known-pattern tagging (parallel ILS, formation, etc.)    │
│  9. Write events → PostGIS separation_events                                │
│     Write alerts  → Redis adsb:alerts pub/sub channel                       │
└──────────┬──────────────────────────┬───────────────────────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────┐    ┌─────────────────────────────────────────────────────┐
│  REDIS           │    │  POSTGRESQL + PostGIS                               │
│                  │    │                                                      │
│  aircraft:{id}   │    │  separation_events  (GIST index on midpoint)        │
│  TTL 60 s        │    │  aircraft_tracks    (sampled positions)             │
│                  │    │  hotspots           (HDBSCAN clusters, recomputed   │
│  alert:{a}:{b}   │    │                      every 60 s by API background   │
│  TTL 5 min       │    │                      task using Shapely convex hull)│
└──────┬───────────┘    └────────────────────────┬────────────────────────────┘
       │                                         │
       └───────────────────┬─────────────────────┘
                           ▼
              ┌────────────────────────┐
              │  FASTAPI  (:8000)      │
              │                        │
              │  GET  /api/live-traffic│  ← Redis scan
              │  GET  /api/near-misses │  ← PostGIS query
              │  GET  /api/hotspots    │  ← PostGIS (GeoJSON)
              │  GET  /api/stats       │  ← parallel gather
              │  WS   /ws/alerts       │  ← Redis pub/sub
              │                        │
              │  Background task:      │
              │  HDBSCAN clustering    │
              │  every 60 s            │
              └───────────┬────────────┘
                          │  HTTP + WebSocket
                          ▼
              ┌────────────────────────┐
              │  FRONTEND  (deck.gl)   │
              │                        │
              │  Live aircraft layer   │  polls every 10 s
              │  Near-miss event dots  │  polls every 30 s
              │  Hotspot polygons      │  polls every 30 s
              │  Real-time alert feed  │  WebSocket push
              └────────────────────────┘
```

### Key design decisions

| Decision | Rationale |
|---|---|
| Geohash precision 4 for candidates | ≈ 20 km cells; 3×3 expansion covers ≈ 60 km — guarantees zero false-negatives at any bearing for the 8 NM threshold |
| Precision 5 stored in DB | Finer cells for PostGIS spatial indexing without affecting detection |
| `foreachBatch` over Spark DStreams | Enables pandas UDFs and psycopg2 batch inserts inside micro-batches |
| asyncpg JSON codec registered at pool creation | `ST_AsGeoJSON()::json` columns are decoded to Python dicts so FastAPI serialises them as GeoJSON objects (not strings) |
| HDBSCAN over DBSCAN | Handles varying-density clusters (tight airport TMAs vs. diffuse en-route corridors) without a global epsilon parameter |

---

## Quick Start

### Prerequisites

- Docker ≥ 24 and Docker Compose ≥ 2.20
- Python ≥ 3.11 (for the host-side simulator and analysis scripts)

```bash
git clone <repo-url>
cd adsb-near-miss
pip install -r requirements.txt   # host-side deps
```

### 1 — Start the stack

```bash
make build   # build the Spark Docker image (first time only, ~2 min)
make up      # start Kafka, PostgreSQL, Redis, Spark, and the FastAPI API
```

`make up` waits for Kafka to be ready, creates the topics, and restarts Spark
so it connects to a live broker.

### 2 — Inject near-miss traffic

```bash
# Lightweight demo: 50 aircraft, guaranteed near-miss every 10 s
make simulate-small

# Full workload: 500 aircraft, 1% natural near-miss rate
make simulate
```

### 3 — Open the map

Navigate to **http://localhost:8000** in your browser.

| Layer | Description |
|---|---|
| Blue dots | Live aircraft (10 s refresh from Redis) |
| Yellow dots | Near-miss / proximity events (30 s refresh) |
| Red dots | Loss-of-separation events |
| Red polygons | HDBSCAN hotspot clusters (30 s refresh, computed every 60 s) |

Hover any dot or polygon for details.  The alert feed in the top-right corner
shows real-time LoS/near-miss events via WebSocket.

### 4 — Explore the API

Interactive docs: **http://localhost:8000/docs**

### 5 — Tear down

```bash
make down          # stop containers, preserve volumes
make down -v       # stop containers and delete all data
```

---

## Makefile reference

| Target | Description |
|---|---|
| `make up` | Start the full stack (Kafka + PostgreSQL + Redis + Spark + API) |
| `make down` | Stop all containers |
| `make build` | Build the Spark Docker image |
| `make simulate-small` | 50-aircraft demo with guaranteed near-miss every 10 s |
| `make simulate` | Full 500-aircraft production-like simulation |
| `make simulate-dry` | Print messages to stdout without Kafka |
| `make spark-logs` | Tail the Spark streaming job log |
| `make spark-restart` | Restart only the Spark job |
| `make spark-clean` | Wipe Spark checkpoints (use after `down -v`) |
| `make api-logs` | Tail the FastAPI log |
| `make api-restart` | Restart the API (picks up code changes; src/ is mounted) |
| `make analyze` | Run one-shot HDBSCAN clustering and write hotspots |
| `make db-reset-events` | Truncate `separation_events` and `hotspots` tables |
| `make test` | Run the pytest suite |
| `make lint` | Run ruff linter |
| `make benchmark` | Run throughput and latency benchmarks |

---

## API reference

All endpoints are available at `http://localhost:8000`.  Interactive Swagger
docs at `/docs`.

### `GET /api/live-traffic`

Returns current aircraft positions as a GeoJSON FeatureCollection.  Data
comes from Redis keys written by the Spark job (TTL 60 s).

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [-98.35, 39.5] },
      "properties": {
        "icao24": "a1b2c3",
        "callsign": "UAL123",
        "altitude_ft": 35000,
        "velocity_kts": 452,
        "true_track": 270
      }
    }
  ]
}
```

### `GET /api/near-misses`

Query parameters:

| Param | Default | Description |
|---|---|---|
| `hours` | `24` | Look-back window (1–720) |
| `severity` | _(all)_ | `PROXIMITY` \| `NEAR_MISS` \| `LOSS_OF_SEPARATION` |
| `limit` | `100` | Max events returned (1–1000) |

### `GET /api/hotspots`

Returns HDBSCAN hotspot cluster polygons as a GeoJSON FeatureCollection.
Clusters are recomputed automatically every 60 s by the API background task.

Query parameters:

| Param | Default | Description |
|---|---|---|
| `min_events` | `3` | Minimum event count to include a cluster |
| `days` | _(all)_ | Only clusters with activity in the last N days |

### `GET /api/stats`

Dashboard summary: event counts by severity, live aircraft count, and top 5
hotspots by event density.

### `WS /ws/alerts`

WebSocket stream of real-time LoS and near-miss alerts.  The Spark job
publishes a JSON alert to the `adsb:alerts` Redis channel whenever it
classifies an event; the API fans it out to all connected clients.

---

## Development

### Run the tests

```bash
make test
# or
python -m pytest tests/ -v
```

The suite covers:

- `tests/test_geohash_partitioner.py` — encode/decode/neighbours/expand
- `tests/test_proximity_detector.py` — haversine, altitude helpers, closure
  rate, and all ICAO separation classification edge cases
- `tests/test_api.py` — every REST endpoint with mock DB and Redis; verifies
  GeoJSON geometry fields are returned as objects (not raw JSON strings)

No Kafka, Spark, PostgreSQL, or Redis instance is required to run the tests.

### Lint

```bash
make lint
# or
python -m ruff check src/ tests/
```

### Real OpenSky data

Export your OpenSky credentials and run the live ingestor:

```bash
export OPENSKY_USERNAME=<user>
export OPENSKY_PASSWORD=<pass>
make ingest
```

---

## Benchmarks

Run the full benchmark suite with `make benchmark`.  Results below are from a
2024 MacBook Pro (M3 Pro, single-core Python 3.12, pure-Python geohash
fallback — no C extension).

### Throughput — full detection pipeline

Measures end-to-end time for one 10-second micro-batch (geohash candidate
generation → self-join → haversine → altitude → proximity filter →
classification).

```
Fleet  │ Median ms │  P95 ms │ Pairs │ Proximity │ Batches/s
───────┼───────────┼─────────┼───────┼───────────┼──────────
   50  │      14.2 │    16.8 │   283 │         0 │      70.4
  100  │      34.6 │    39.1 │  1027 │         0 │      28.9
  200  │     103.8 │   115.4 │  3841 │         0 │       9.6
  500  │     617.3 │   651.2 │ 17204 │         0 │       1.6
```

Notes:
- **Proximity = 0** for random US traffic because aircraft are spread across
  ~8 million km²; near-miss events only appear when the simulator injects
  converging pairs.
- Candidate pairs grow roughly as O(n²) because dense airspace produces more
  same-cell collisions after geohash expansion.  Installing the `python-geohash`
  C extension typically cuts geohash time by 3–5×.
- In production the Spark batch interval is 10 s; a fleet of 200 aircraft
  completes well within the budget at ~104 ms.

### Latency — individual functions

P50 / P95 / P99 latencies for the hottest code paths:

```
Function                         │   Calls │   P50 µs │   P95 µs │   P99 µs │   Max µs
─────────────────────────────────┼─────────┼──────────┼──────────┼──────────┼─────────
haversine                        │  200000 │     1.84 │     2.53 │     3.28 │    19.41
classify_separation              │  200000 │     0.45 │     0.63 │     0.89 │     8.14
normalize_altitude (both)        │  200000 │     0.33 │     0.47 │     0.66 │     6.03
normalize_altitude (baro)        │  200000 │     0.28 │     0.41 │     0.57 │     5.18
vertical_separation              │  200000 │     0.31 │     0.44 │     0.60 │     4.87
closure_rate_with_positions      │  200000 │     1.97 │     2.71 │     3.61 │    22.34
gh_encode (precision 4)          │  200000 │     5.12 │     6.74 │     8.93 │    51.22
gh_expand (precision 4)          │  200000 │    49.83 │    64.17 │    82.40 │   341.06
```

`gh_expand` dominates because it calls `gh_encode` nine times (center +
8 neighbours).  This is the main optimisation target; installing
`python-geohash` reduces it to ~12 µs at P50.

Reproduce on your hardware:

```bash
python benchmarks/latency_test.py --calls 200000
python benchmarks/throughput_test.py --fleet 50 100 200 500
```

---

## Project layout

```
adsb-near-miss/
├── docker-compose.yml
├── Makefile
├── requirements.txt
│
├── infra/
│   ├── kafka/          Kafka KRaft configuration
│   ├── postgres/       init.sql — PostGIS schema
│   ├── redis/          redis.conf
│   ├── spark/          Dockerfile (PySpark 3.5 + deps)
│   └── api/            Dockerfile (FastAPI)
│
├── src/
│   ├── ingestion/
│   │   ├── adsb_simulator.py    Synthetic ADS-B traffic generator
│   │   └── opensky_producer.py  Live OpenSky → Kafka ingestor
│   │
│   ├── processing/
│   │   ├── spark_streaming_job.py   Main Spark foreachBatch handler
│   │   ├── geohash_partitioner.py   Encode / decode / expand helpers
│   │   └── proximity_detector.py    Haversine, ICAO classification
│   │
│   ├── analysis/
│   │   └── hotspot_clustering.py    HDBSCAN + Shapely convex-hull writer
│   │
│   └── api/
│       ├── main.py                  FastAPI app + asyncpg pool + background task
│       └── routes/
│           ├── live_traffic.py
│           ├── near_misses.py
│           ├── hotspots.py
│           ├── stats.py
│           └── ws_alerts.py
│
├── frontend/
│   └── index.html      Single-page deck.gl + MapLibre map
│
├── tests/
│   ├── test_geohash_partitioner.py
│   ├── test_proximity_detector.py
│   └── test_api.py
│
└── benchmarks/
    ├── throughput_test.py
    └── latency_test.py
```

---

## Data sources

### Simulator (default)

`make simulate-small` starts the built-in Python simulator which generates
realistic ADS-B messages for 50 aircraft flying random US routes and injects
guaranteed near-miss pairs every 10 seconds.  No external accounts required.

### OpenSky Network (live data)

The [OpenSky Network](https://opensky-network.org) provides a free REST API
with real ADS-B data.  Register for a free account, export your credentials,
and run `make ingest`.

> **Privacy note**: ADS-B data is passively received radio-frequency
> information broadcast publicly by aircraft transponders.  No private data
> is collected or stored.

---

## License

MIT
