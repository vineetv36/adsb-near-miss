# ADS-B Near-Miss Detection & Airspace Congestion Analyzer

## Project Overview

A real-time streaming data pipeline that ingests live ADS-B aircraft position data, detects loss-of-separation events between aircraft, identifies recurring geographic hotspots where near-misses cluster, and produces evidence-backed analysis to support airspace safety policy advocacy.

---

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────────────┐
│  OpenSky API    │────▶│   Kafka     │────▶│  Spark Structured        │
│  (or RTL-SDR)   │     │  (partitioned│     │  Streaming               │
│                 │     │  by geohash) │     │                          │
└─────────────────┘     └─────────────┘     │  1. Geohash partitioning │
                                            │  2. Candidate pair pruning│
                                            │  3. Haversine + altitude  │
                                            │  4. Separation scoring    │
                                            └──────┬───────┬───────┬───┘
                                                   │       │       │
                                          ┌────────▼──┐ ┌──▼───┐ ┌─▼────────┐
                                          │  PostGIS  │ │Redis │ │ Parquet  │
                                          │ (hotspot  │ │(live │ │ (cold    │
                                          │  queries) │ │state)│ │  store)  │
                                          └─────┬─────┘ └──┬───┘ └─┬────────┘
                                                │          │       │
                                          ┌─────▼──────────▼───────▼─────┐
                                          │         FastAPI               │
                                          │  /api/live-traffic (GeoJSON) │
                                          │  /api/near-misses            │
                                          │  /api/hotspots               │
                                          └──────────────┬───────────────┘
                                                         │
                                          ┌──────────────▼───────────────┐
                                          │  Deck.gl / Kepler.gl         │
                                          │  Live map + hotspot heatmap  │
                                          └──────────────────────────────┘
```

---

## File Structure

```
adsb-near-miss/
│
├── docker-compose.yml              # Full stack orchestration
├── Makefile                         # dev shortcuts: make up, make ingest, make test
├── README.md                        # Project overview, architecture, quickstart
├── .env.example                     # OpenSky credentials, config defaults
│
├── infra/
│   ├── kafka/
│   │   └── docker-compose.kafka.yml # Kafka + Zookeeper (or KRaft) config
│   ├── postgres/
│   │   ├── init.sql                 # PostGIS extensions, schema, indexes
│   │   └── Dockerfile               # postgres + postgis image
│   └── redis/
│       └── redis.conf
│
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── opensky_producer.py      # Polls OpenSky API → Kafka
│   │   ├── adsb_simulator.py        # Generates synthetic ADS-B for testing
│   │   ├── rtlsdr_producer.py       # (Optional) dump1090 → Kafka for live antenna
│   │   └── schemas.py               # Avro/JSON schema definitions
│   │
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── spark_streaming_job.py   # Main Spark Structured Streaming app
│   │   ├── geohash_partitioner.py   # Geohash encoding + neighbor cell logic
│   │   ├── proximity_detector.py    # Haversine + altitude separation calc
│   │   ├── known_patterns.py        # Filters: parallel approaches, departures, etc.
│   │   └── deduplication.py         # Multi-ground-station dedup logic
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── postgis_writer.py        # Write near-miss events + aircraft tracks
│   │   ├── redis_state.py           # Current aircraft positions, live alerts
│   │   └── parquet_writer.py        # Cold storage for batch analytics
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py                  # FastAPI app
│   │   ├── routes/
│   │   │   ├── live_traffic.py      # GET /api/live-traffic → GeoJSON
│   │   │   ├── near_misses.py       # GET /api/near-misses?hours=24
│   │   │   └── hotspots.py          # GET /api/hotspots?min_events=10
│   │   └── models.py                # Pydantic response models
│   │
│   └── analysis/
│       ├── __init__.py
│       ├── hotspot_clustering.py    # DBSCAN/HDBSCAN on near-miss coords
│       ├── temporal_patterns.py     # Time-of-day, day-of-week analysis
│       ├── airspace_overlay.py      # FAA airspace class boundaries
│       └── policy_report.py         # Generates summary stats for advocacy
│
├── notebooks/
│   ├── 01_exploratory.ipynb         # Initial data exploration
│   ├── 02_hotspot_analysis.ipynb    # Clustering + visualization
│   └── 03_policy_brief.ipynb        # Final analysis with maps + stats
│
├── frontend/
│   ├── index.html                   # Deck.gl / Kepler.gl map
│   ├── app.js                       # WebSocket connection, layer rendering
│   └── style.css
│
├── tests/
│   ├── conftest.py                  # Shared fixtures (sample ADS-B data)
│   ├── test_geohash.py             # Geohash encoding + neighbor logic
│   ├── test_proximity.py           # Distance calc edge cases
│   ├── test_dedup.py               # Deduplication correctness
│   ├── test_known_patterns.py      # Filter accuracy
│   ├── test_api.py                 # FastAPI endpoint tests
│   └── test_integration.py         # End-to-end with embedded Kafka
│
├── benchmarks/
│   ├── throughput_test.py           # Measure events/sec at varying loads
│   └── latency_test.py             # e2e latency: ingest → alert
│
├── docs/
│   ├── ARCHITECTURE.md              # Detailed design decisions + tradeoffs
│   ├── BENCHMARKS.md                # Performance results with charts
│   └── POLICY_ANALYSIS.md           # Findings summary for non-technical readers
│
├── scripts/
│   ├── download_faa_airspace.py     # Fetch FAA airspace shapefiles
│   ├── download_airports.py         # Fetch airport coordinates
│   └── seed_historical.py          # Backfill from OpenSky historical API
│
└── pyproject.toml                   # Project deps (Poetry or pip)
```

---

## Module Specifications

### 1. Ingestion: `src/ingestion/opensky_producer.py`

**Purpose:** Poll the OpenSky Network REST API and produce aircraft state vectors to Kafka.

**Behavior:**
- Poll `https://opensky-network.org/api/states/all` every 10 seconds (authenticated) or 15 seconds (anonymous)
- Each state vector contains: icao24, callsign, origin_country, longitude, latitude, baro_altitude, geo_altitude, velocity, true_track, vertical_rate, on_ground, squawk, time_position, last_contact
- Produce each aircraft state as a JSON message to Kafka topic `adsb.raw`
- Partition key: geohash of (lat, lon) at precision 4 (roughly 40km x 20km cells) — this ensures aircraft in the same geographic area land on the same partition for downstream co-processing
- Handle rate limits with exponential backoff
- Log ingestion rate (messages/sec) and API response latency to stdout for Prometheus scraping
- Skip aircraft where on_ground=true (not relevant for airspace separation)

**Key config (via env vars):**
- `OPENSKY_USERNAME` / `OPENSKY_PASSWORD` (optional, increases rate limit)
- `KAFKA_BOOTSTRAP_SERVERS`
- `POLL_INTERVAL_SECONDS` (default: 10)
- `BOUNDING_BOX` (optional, e.g., US lower 48: `24.396,-125.0,49.384,-66.934`)

---

### 2. Ingestion: `src/ingestion/adsb_simulator.py`

**Purpose:** Generate realistic synthetic ADS-B data for development and demos when no live data is available (e.g., no snow storms, no antenna).

**Behavior:**
- Simulate N aircraft (default: 500) flying realistic routes between major US airports
- Each aircraft follows a great-circle route with realistic speed (250-500 kts), altitude profiles (climb, cruise, descend), and position update intervals (1-2 sec)
- Periodically inject "near-miss scenarios": two aircraft converging on the same point at similar altitudes
- Inject realistic noise: GPS jitter (±50m), occasional missed pings, altitude reporting inconsistencies (baro vs geo offset)
- Produce to the same `adsb.raw` Kafka topic with identical schema
- Configurable scenario density: `--near-miss-rate 0.01` means ~1% of time windows contain a near-miss event

---

### 3. Processing: `src/processing/spark_streaming_job.py`

**Purpose:** Main Spark Structured Streaming application. Consumes raw ADS-B, detects proximity events, writes results downstream.

**Processing steps per micro-batch (10-second tumbling window):**

1. **Deserialize + validate:** Parse JSON, drop malformed records to a dead-letter topic (`adsb.dlq`)
2. **Deduplicate:** Same aircraft reported by multiple ground stations → keep the record with the most recent `time_position`
3. **Geohash assignment:** Compute geohash at precision 5 (~5km cells) for each aircraft. Also compute the 8 neighboring geohash cells.
4. **Self-join for candidate pairs:** Join the dataset against itself on matching geohash OR neighboring geohash. This reduces the O(n²) pairwise comparison to only aircraft in nearby cells. Filter out self-pairs (same icao24).
5. **Precise distance computation:** For each candidate pair, compute:
   - Horizontal distance: Haversine formula on (lat1, lon1) → (lat2, lon2) in nautical miles
   - Vertical separation: |altitude1 - altitude2| in feet
   - Closure rate: Estimated from velocity vectors (are they converging or diverging?)
6. **Separation classification:**
   - **Loss of separation (LoS):** < 3 NM horizontal AND < 1000 ft vertical (en route) or < 1 NM / 500 ft (terminal)
   - **Near miss:** < 5 NM horizontal AND < 1500 ft vertical
   - **Proximity alert:** < 8 NM horizontal AND < 2000 ft vertical
7. **Known-pattern filtering:** Run through `known_patterns.py` to tag (not discard) events that match expected patterns (parallel ILS approaches, military formations, helicopter operations below 500 ft)
8. **Output:** Write flagged events to three sinks:
   - PostGIS: Full event record with geometries
   - Redis: Lightweight alert for live dashboard (TTL: 5 minutes)
   - Parquet: Append to partitioned cold storage (partitioned by date and geohash)

**Key design decisions to document in ARCHITECTURE.md:**
- Why geohash precision 5 (tradeoff between cell size and false-negative rate at borders)
- Why tumbling windows vs. sliding windows (simplicity, exactly-once semantics)
- How border-crossing is handled (aircraft in cell A, neighbor in cell B, distance < threshold)
- Memory management for the self-join at scale

---

### 4. Processing: `src/processing/geohash_partitioner.py`

**Purpose:** Geohash encoding and neighbor-cell logic.

**Functions:**
- `encode(lat, lon, precision=5) -> str` — Standard geohash encoding
- `neighbors(geohash: str) -> List[str]` — Returns the 8 adjacent cells
- `decode_bounds(geohash: str) -> BBox` — Returns bounding box for a cell
- `expand(geohash: str) -> List[str]` — Returns the cell + its 8 neighbors (9 total)

**Why not use an existing library?** You can use `python-geohash`, but wrapping it lets you add precision-aware border handling and unit test the neighbor logic explicitly — both good talking points in interviews.

---

### 5. Processing: `src/processing/proximity_detector.py`

**Purpose:** Core distance and separation calculation.

**Functions:**
- `haversine(lat1, lon1, lat2, lon2) -> float` — Returns distance in nautical miles
- `vertical_separation(alt1, alt2) -> float` — Returns separation in feet, handling null/mixed altitude sources
- `closure_rate(velocity1, track1, velocity2, track2) -> float` — Estimated rate of closure in knots (positive = converging)
- `classify_separation(horizontal_nm, vertical_ft, airspace_class) -> SeparationLevel` — Returns enum: NORMAL, PROXIMITY, NEAR_MISS, LOSS_OF_SEPARATION
- `normalize_altitude(baro_alt, geo_alt, qnh=None) -> float` — Reconcile barometric vs geometric altitude

**Edge cases to handle:**
- One or both aircraft missing altitude data → flag as UNKNOWN, don't discard
- Aircraft in climb/descent (rapidly changing altitude) → use interpolated altitude at the closest-approach time
- Supersonic closure rates (military jets) → different separation standards

---

### 6. Processing: `src/processing/known_patterns.py`

**Purpose:** Tag proximity events that match expected, safe flight patterns to avoid polluting the hotspot analysis.

**Patterns to detect:**
- **Parallel ILS approaches:** Two aircraft within 3 NM horizontally but on parallel runways (check bearing relative to runway heading ± 10°, both descending, within 15 NM of an airport)
- **Departure corridor:** Aircraft climbing through similar airspace within 2 minutes of takeoff from the same airport
- **Holding patterns:** Aircraft in a racetrack pattern at assigned altitudes (detect from track angle rate-of-change)
- **Formation flight:** Military aircraft with very small separation but matching velocity vectors (closure rate ≈ 0)
- **Helicopter operations:** Either aircraft below 500 ft AGL with low velocity (< 100 kts)

Each event gets tagged with `pattern: str | None` — downstream analysis can then filter or segment by pattern.

---

### 7. Storage: `infra/postgres/init.sql`

**Schema:**

```sql
CREATE EXTENSION IF NOT EXISTS postgis;

-- Raw aircraft positions (sampled, not every ping)
CREATE TABLE aircraft_tracks (
    id              BIGSERIAL PRIMARY KEY,
    icao24          VARCHAR(6) NOT NULL,
    callsign        VARCHAR(8),
    geom            GEOMETRY(Point, 4326) NOT NULL,
    altitude_ft     FLOAT,
    velocity_kts    FLOAT,
    true_track      FLOAT,
    vertical_rate   FLOAT,
    geohash         VARCHAR(8),
    observed_at     TIMESTAMPTZ NOT NULL
);

-- Near-miss / separation events
CREATE TABLE separation_events (
    id              BIGSERIAL PRIMARY KEY,
    icao24_a        VARCHAR(6) NOT NULL,
    icao24_b        VARCHAR(6) NOT NULL,
    callsign_a      VARCHAR(8),
    callsign_b      VARCHAR(8),
    geom_a          GEOMETRY(Point, 4326) NOT NULL,
    geom_b          GEOMETRY(Point, 4326) NOT NULL,
    midpoint        GEOMETRY(Point, 4326) NOT NULL,  -- For hotspot clustering
    horizontal_nm   FLOAT NOT NULL,
    vertical_ft     FLOAT NOT NULL,
    closure_rate_kt FLOAT,
    severity        VARCHAR(20) NOT NULL,  -- PROXIMITY, NEAR_MISS, LOSS_OF_SEPARATION
    pattern         VARCHAR(30),           -- NULL = unrecognized, or known pattern name
    altitude_a_ft   FLOAT,
    altitude_b_ft   FLOAT,
    observed_at     TIMESTAMPTZ NOT NULL
);

-- Hotspot clusters (computed by batch analysis)
CREATE TABLE hotspots (
    id              SERIAL PRIMARY KEY,
    geom            GEOMETRY(Polygon, 4326) NOT NULL,  -- Convex hull of cluster
    centroid        GEOMETRY(Point, 4326) NOT NULL,
    event_count     INT NOT NULL,
    avg_severity    FLOAT,
    dominant_pattern VARCHAR(30),
    airspace_class  VARCHAR(2),
    nearest_airport VARCHAR(4),  -- ICAO code
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ,
    computed_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Spatial indexes
CREATE INDEX idx_tracks_geom ON aircraft_tracks USING GIST(geom);
CREATE INDEX idx_tracks_time ON aircraft_tracks(observed_at);
CREATE INDEX idx_events_midpoint ON separation_events USING GIST(midpoint);
CREATE INDEX idx_events_time ON separation_events(observed_at);
CREATE INDEX idx_events_severity ON separation_events(severity);
CREATE INDEX idx_hotspots_geom ON hotspots USING GIST(geom);
```

---

### 8. API: `src/api/`

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/live-traffic` | GeoJSON FeatureCollection of current aircraft positions from Redis |
| GET | `/api/near-misses?hours=24&severity=NEAR_MISS` | Recent separation events, filterable |
| GET | `/api/near-misses/{id}` | Single event with full detail + both aircraft tracks |
| GET | `/api/hotspots?min_events=10&days=30` | Clustered hotspot polygons as GeoJSON |
| GET | `/api/hotspots/{id}/events` | All events within a hotspot |
| GET | `/api/stats` | Dashboard stats: events/hour, top hotspots, coverage area |
| WS | `/ws/alerts` | WebSocket push for real-time LoS/near-miss alerts |

---

### 9. Analysis: `src/analysis/hotspot_clustering.py`

**Purpose:** Run DBSCAN or HDBSCAN over historical near-miss event midpoints to identify recurring geographic clusters.

**Approach:**
- Query all `separation_events` with `severity IN ('NEAR_MISS', 'LOSS_OF_SEPARATION')` and `pattern IS NULL` (exclude known-safe patterns)
- Convert midpoint coordinates to a distance matrix using Haversine
- Run HDBSCAN with `min_cluster_size=5`, `min_samples=3`, `metric='haversine'`
- For each cluster: compute convex hull, centroid, event count, temporal distribution, dominant aircraft types
- Cross-reference with FAA airspace shapefiles to tag airspace class (B, C, D, E, G)
- Find nearest airport for each cluster
- Write results to `hotspots` table

---

### 10. Analysis: `notebooks/03_policy_brief.ipynb`

**Purpose:** The "so what" deliverable. A Jupyter notebook that produces publication-ready maps and statistics.

**Contents:**
- National heatmap of near-miss density (Kepler.gl embed)
- Top 10 hotspots table: location, event count, airspace class, nearest airport, peak times
- Time-of-day distribution: when do near-misses peak?
- Airspace class breakdown: what % of events occur in uncontrolled (Class E/G) vs controlled airspace?
- Trend analysis: are events increasing over time?
- Case studies: 2-3 specific hotspots with narrative explanation of why they're dangerous
- Policy recommendations with supporting data

---

## Key Engineering Decisions to Document

These go in `docs/ARCHITECTURE.md` and are the things a FAANG interviewer will ask about:

1. **Why geohash precision 5?** Cells are ~5km × 5km. Separation thresholds max at ~8 NM (15 km), so checking a cell + its 8 neighbors covers 15km in every direction. Precision 4 (40km cells) creates too many false candidates; precision 6 (1km cells) requires checking too many neighbors.

2. **Why Kafka partitioned by geohash?** Co-locates aircraft in the same region on the same partition, enabling partition-local joins in Spark. Reduces shuffle overhead.

3. **Why tumbling windows, not sliding?** Simpler watermark management, easier exactly-once semantics. The 10-second window matches the API poll interval. Sliding windows would catch events spanning window boundaries but add complexity — document this tradeoff.

4. **Why PostGIS + Redis + Parquet (three sinks)?** PostGIS for complex spatial queries (hotspot analysis, airspace overlay). Redis for sub-ms reads on the live dashboard. Parquet for cheap long-term storage and Spark batch analytics. Each store serves a different access pattern. This is a classic FAANG system design pattern worth discussing.

5. **Why HDBSCAN over DBSCAN?** HDBSCAN handles varying-density clusters better — some hotspots will be tight (airport vicinity) and others will be diffuse (en-route corridors). No need to tune epsilon globally.

6. **Altitude normalization:** ADS-B reports barometric altitude (pressure-referenced) and geometric altitude (GPS-referenced). The difference can be hundreds of feet depending on local pressure. Document how you reconcile them and what happens when one is missing.

---

## Benchmarks to Capture

Run these and put real numbers in `docs/BENCHMARKS.md` — and then put them on your resume:

| Metric | How to measure | Resume-ready phrasing |
|--------|---------------|----------------------|
| Ingestion throughput | `benchmarks/throughput_test.py` with simulator at max rate | "Ingested X aircraft state vectors/sec" |
| End-to-end latency | `benchmarks/latency_test.py`: timestamp at ingest → timestamp at Redis write | "p99 end-to-end latency of Xms from ingest to alert" |
| Proximity detection throughput | Measure candidate pairs evaluated per second in Spark | "Evaluated X pairwise proximity checks/sec" |
| API response time | Load test FastAPI with `locust` or `wrk` | "Served geospatial queries at p99 Xms" |
| Geohash pruning efficiency | Log candidate pairs vs. total possible pairs | "Reduced O(n²) pairwise search by X% via geohash spatial indexing" |
| Storage write throughput | Measure events/sec to PostGIS under load | "Sustained X events/sec writes to PostGIS" |

---

## Quickstart (for README.md)

```bash
# Clone and start the full stack
git clone https://github.com/boblaw/adsb-near-miss.git
cd adsb-near-miss
cp .env.example .env  # Add OpenSky credentials (optional)

# Start everything
docker compose up -d

# Start ingesting (simulator mode for demo)
make simulate

# Or use live OpenSky data
make ingest

# Open the dashboard
open http://localhost:8080

# Run the hotspot analysis
make analyze

# Run tests
make test

# Run benchmarks
make benchmark
```

---

## Dependencies

```toml
[tool.poetry.dependencies]
python = "^3.11"
pyspark = "^3.5"
kafka-python = "^2.0"
confluent-kafka = "^2.3"
fastapi = "^0.109"
uvicorn = "^0.27"
redis = "^5.0"
psycopg2-binary = "^2.9"
sqlalchemy = "^2.0"
geoalchemy2 = "^0.14"
python-geohash = "^0.8"
hdbscan = "^0.8"
pyarrow = "^15.0"
pydantic = "^2.5"
httpx = "^0.27"
websockets = "^12.0"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
pytest-asyncio = "^0.23"
locust = "^2.20"
jupyter = "^1.0"
keplergl = "^0.3"
matplotlib = "^3.8"
```

---

## Claude Code Prompting Strategy

When handing this to Claude Code, work through it in this order:

1. **Start with infra:** "Set up docker-compose.yml with Kafka, PostgreSQL+PostGIS, and Redis. Create the init.sql schema."
2. **Build the simulator:** "Create adsb_simulator.py that generates realistic aircraft state vectors and produces to Kafka. Include near-miss scenario injection."
3. **Build the core processing:** "Create the Spark Structured Streaming job with geohash partitioning and proximity detection. Write to PostGIS and Redis."
4. **Build the API:** "Create the FastAPI app with GeoJSON endpoints for live traffic, near-misses, and hotspots."
5. **Build the analysis:** "Create the HDBSCAN hotspot clustering job and the policy analysis notebook."
6. **Build the frontend:** "Create a Deck.gl map that shows live traffic and hotspot heatmap layers."
7. **Tests and benchmarks last:** "Write tests for geohash, proximity detection, and API endpoints. Create throughput and latency benchmarks."

This order ensures each layer has its dependencies ready before you build it.
