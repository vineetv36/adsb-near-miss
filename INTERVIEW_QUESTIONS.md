# Data Engineer Interview Questions — ADS-B Near-Miss Detector

Questions and answers a data engineering interviewer could ask about this
project.  Topics span streaming architecture, spatial data, distributed systems,
and Python performance engineering.

---

## Table of Contents

1. [Kafka & Streaming Fundamentals](#1-kafka--streaming-fundamentals)
2. [Spark Structured Streaming](#2-spark-structured-streaming)
3. [Spatial Data & Geohash](#3-spatial-data--geohash)
4. [PostGIS & Database Design](#4-postgis--database-design)
5. [Redis — Caching & Pub/Sub](#5-redis--caching--pubsub)
6. [HDBSCAN Hotspot Clustering](#6-hdbscan-hotspot-clustering)
7. [FastAPI & asyncpg](#7-fastapi--asyncpg)
8. [System Design & Architecture Trade-offs](#8-system-design--architecture-trade-offs)
9. [Testing & Observability](#9-testing--observability)
10. [Performance & Scalability](#10-performance--scalability)

---

## 1. Kafka & Streaming Fundamentals

### Q1. Why does this project use Kafka KRaft mode instead of the traditional Zookeeper-based deployment?

**Answer:**
KRaft (Kafka Raft) removes the dependency on Apache ZooKeeper, which had been a
significant operational burden: it required a separate 3-node quorum for HA,
added latency to leader election, and had its own monitoring story.  In KRaft
mode a single Kafka broker self-manages metadata using an internal Raft log.
For a development/demo setup this means one less container, faster startup, and
no `KAFKA_ZOOKEEPER_CONNECT` variable to manage.

In production you would still run a 3-broker KRaft cluster for fault tolerance,
but the operational complexity is lower than maintaining a parallel ZooKeeper
ensemble.

---

### Q2. The `adsb.raw` topic is created with 6 partitions. How did you arrive at that number, and what is the implication for Spark consumer parallelism?

**Answer:**
Six partitions is a deliberate choice:

- **Parallelism ceiling**: Spark's Kafka source creates one task per partition
  per micro-batch by default.  Six partitions means up to 6 concurrent reader
  tasks.  For a 10-second micro-batch on a `local[2]` Spark instance the
  bottleneck is CPU, not I/O, so 6 partitions is already more than enough
  headroom.
- **Scalability reserve**: When deployed on a multi-node Spark cluster you want
  at least as many partitions as executor cores dedicated to this job.  Six
  partitions supports up to a 6-core cluster without repartitioning.
- **Retention cost**: Each partition is replicated `replication.factor=1` (demo
  mode) and retained for 1 hour.  With a 500-aircraft fleet producing ~500
  JSON messages per 10-second batch, the total write rate is ~50 msg/s — tiny
  compared to Kafka's capacity per partition.

If you wanted sub-second latency on a multi-node cluster you would increase
partitions to match executor count and decrease the Spark trigger interval.

---

### Q3. What is the Dead Letter Queue topic (`adsb.dlq`) and why is it important?

**Answer:**
The DLQ (`adsb.dlq`, 1 partition) captures messages that fail to parse or
violate schema constraints — malformed JSON, unexpected field types, missing
required keys.  Without a DLQ these messages would either:

1. **Silently drop** (if you catch and ignore exceptions) — you lose data and
   have no visibility into upstream quality issues.
2. **Poison-pill the consumer** (if you let exceptions propagate) — Spark will
   retry the same batch forever, stalling all downstream processing.

The DLQ pattern solves both: bad records are forwarded to `adsb.dlq` with their
raw bytes and an error reason, allowing a separate quality-monitoring job to
inspect failures without blocking the main pipeline.

In this codebase the Spark job sends records to the DLQ when JSON decoding
raises an exception, preserving the raw payload for post-mortem analysis.

---

### Q4. The `docker-compose.yml` configures two Kafka listeners: `INTERNAL://kafka:9092` and `EXTERNAL://localhost:29092`. Why are two listeners needed?

**Answer:**
Kafka listeners control how clients discover and connect to brokers.  Two are
needed because there are two network contexts:

| Listener | Used by | Network |
|---|---|---|
| `INTERNAL://kafka:9092` | Spark job, FastAPI API (inside Docker network) | Docker bridge (`adsb-net`) |
| `EXTERNAL://localhost:29092` | `adsb_simulator.py`, `opensky_producer.py` (host machine) | Host loopback |

A container-to-container client (Spark) resolves `kafka` via Docker DNS to the
container's internal IP.  A host-side client can't resolve `kafka` — it must
use `localhost:29092`, which is port-forwarded to the container.

If you used only one listener and advertised `kafka:9092`, host-side producers
would get a metadata response pointing to `kafka:9092`, which they can't reach,
and connections would silently fail after the initial handshake.

---

### Q5. How does the simulator guarantee a near-miss event every 10 seconds in `make simulate-small`?

**Answer:**
`adsb_simulator.py` maintains a small pool of "injection pairs" — aircraft
whose positions are programmatically set to converge within 3 NM / 800 ft of
each other on each publish cycle.  Every 10 seconds the simulator:

1. Advances all regular aircraft positions using realistic physics
   (`heading`, `velocity`, elapsed wall-clock time via `last_ping_t`).
2. Overwrites the injection pair's positions with coordinates that are exactly
   2.5 NM apart at the same altitude, triggering a `LOSS_OF_SEPARATION` event
   in the Spark pipeline.
3. Publishes both frames to Kafka in the same batch so Spark sees them in the
   same micro-batch window.

This injection mechanism is independent of random fleet positions, so you get
reliable near-misses for demo and testing purposes regardless of how the 50
background aircraft happen to be distributed across the US.

---

## 2. Spark Structured Streaming

### Q6. Why was `foreachBatch` chosen over `foreach` or Spark's native write sinks?

**Answer:**
`foreachBatch` receives a complete micro-batch as a Spark DataFrame (or, in
this case, a pandas DataFrame after `toPandas()`).  This unlocks three things
that are impossible or impractical with `foreach` or native sinks:

1. **Multi-sink writes in one function**: A single `foreachBatch` call writes to
   Redis AND PostGIS AND a Parquet archive.  Native sinks (`writeStream`) only
   support one output per query.
2. **Pandas UDFs and arbitrary Python libraries**: `haversine`, `psycopg2`
   batch inserts, and `geohash` encoding run inside the foreachBatch function
   on the driver/executor without needing to register Spark SQL UDFs.
3. **Transactional-style logic**: The geohash self-join, proximity filter, and
   classification are implemented as pandas operations that would be awkward to
   express in Spark's relational API (especially the 3×3 neighbourhood
   expansion).

The trade-off is that `foreachBatch` runs on the Spark driver for small batches
when using `toPandas()`, limiting parallelism.  For this workload (≤500
aircraft, ~104 ms at fleet=200) the driver is the right place.

---

### Q7. What is Spark checkpointing and why must you run `make spark-clean` after `make down -v`?

**Answer:**
Spark Structured Streaming writes a **checkpoint directory** (configured via
`checkpointLocation`) that stores:

- **Offsets**: the last committed Kafka offset per partition, so the job can
  resume from exactly where it left off after a restart.
- **Metadata**: the streaming query plan and schema, ensuring schema evolution
  is detected.
- **WAL**: the write-ahead log of completed micro-batches for exactly-once
  delivery to sinks.

When you run `make down -v` you delete the PostgreSQL and Kafka volumes,
destroying all data and resetting Kafka partition offsets to zero.  The
checkpoint still contains the *old* offsets.  On the next `make up`, Spark
tries to seek Kafka to those stale offsets, gets `OFFSET_OUT_OF_RANGE`, and
either fails or skips to `latest` depending on `auto.offset.reset`.

`make spark-clean` deletes the checkpoint directory, forcing Spark to start a
fresh streaming query from `latest` (or `earliest`, whichever is configured),
which is consistent with the freshly reset Kafka state.

---

### Q8. The Spark job uses `local[2]` as the master. What does this mean and what are its production limitations?

**Answer:**
`local[2]` runs Spark in local mode with 2 worker threads in the same JVM
process as the driver.  This means:

- No cluster manager (no YARN, no Kubernetes, no Mesos).
- Maximum parallelism = 2 tasks at a time.
- The executor and driver share memory in one process.

**Production limitations:**
- **No fault tolerance**: if the single JVM crashes, the streaming job dies.
- **No horizontal scaling**: you can't add nodes to increase throughput.
- **Shared memory pressure**: large batches can OOM the driver since there are
  no executor-side JVMs to offload work to.
- **toPandas() bottleneck**: calling `toPandas()` on a distributed DataFrame
  brings all data to the driver — fine for 500 aircraft but problematic for
  hundreds of thousands.

For production you would deploy on Kubernetes with `spark-on-k8s`, configure
proper executor memory and core counts, and use vectorised pandas UDFs
(`mapInPandas`) instead of `toPandas()` to keep data on executors.

---

### Q9. Explain the deduplication logic in the Spark batch: "newest ping per icao24". Why is it necessary?

**Answer:**
ADS-B receivers sometimes publish duplicate messages for the same aircraft in
quick succession (multiple ground stations picking up the same transponder
ping).  Without deduplication, a single aircraft could appear twice in a
micro-batch with slightly different coordinates, causing a phantom self-pair
during the geohash self-join.

The dedup strategy:
1. Parse the `time_position` field (Unix timestamp from the transponder).
2. Group by `icao24`, keep only the row with the maximum `time_position`.

This is a "last-write-wins" policy per aircraft per micro-batch, which is
correct because a newer ping is always more authoritative than an older one.
A secondary benefit is that it bounds the DataFrame size: regardless of how
many duplicate pings arrive, the pipeline always processes at most N rows for
N unique aircraft.

---

### Q10. How does the Spark job achieve exactly-once semantics to PostGIS?

**Answer:**
Strictly speaking, `foreachBatch` with a custom psycopg2 writer provides
**at-least-once** delivery by default, not exactly-once.  Exactly-once to
PostGIS would require an idempotent write strategy:

- **Option 1 — Idempotent upsert**: use a deterministic event ID (e.g.,
  `SHA256(icao24_a || icao24_b || observed_at)`) as the primary key and
  `INSERT ... ON CONFLICT DO NOTHING`.  A redelivered micro-batch inserts
  nothing on conflict.
- **Option 2 — Transactional checkpointing**: write Kafka offsets and event
  rows in the same PostgreSQL transaction.  If the commit fails, Spark retries
  from the last committed offset, producing exactly the same rows.

The current implementation uses a surrogate auto-increment `id` and does not
deduplicate on retry.  In a demo context this is acceptable; a duplicate event
row is a minor data quality issue, not a safety hazard.  For production you
would add the deterministic ID approach.

---

## 3. Spatial Data & Geohash

### Q11. Why was geohash precision 4 chosen for candidate generation? What would happen if you used precision 5 or precision 3?

**Answer:**
**Geohash precision 4** produces cells approximately 39 km wide × 20 km tall
(~20 km "effective radius").  With 3×3 neighbourhood expansion the coverage is
~60 km — well beyond the 8 NM (~15 km) detection threshold.  This guarantees
**zero false-negatives**: any pair within 8 NM must share at least one of the
9 expanded cells.

**Precision 5** (~4.9 km × 4.9 km cells):
- Expansion covers only ~15 km — marginally covers the 8 NM threshold at
  cardinal bearings but could miss diagonal pairs near corners.
- You would need 5×5 expansion (25 cells) to safely cover 8 NM, multiplying
  candidate pairs by 25/9 ≈ 2.8×.

**Precision 3** (~156 km × 156 km cells):
- Expansion covers ~470 km — far more than needed.
- Every pair of aircraft in a 470 km radius becomes a candidate, producing
  O(n²) pairs even for aircraft far apart.  For a 500-aircraft fleet spread
  across the US this would generate millions of pairs per batch instead of
  ~17,000.

Precision 4 is the sweet spot: enough coverage for guaranteed correctness,
small enough cells to filter out the vast majority of non-proximate pairs
early.

---

### Q12. Walk me through the math that proves the 3×3 geohash expansion guarantees no false negatives at 8 NM.

**Answer:**
A geohash-4 cell at mid-latitudes (≈ 40° N, typical for continental US) is
approximately:

- Width: 39 km (longitude dimension, shrinks with cos(lat))
- Height: 20 km (latitude dimension, fixed by precision)

The smallest possible "safe radius" of a cell is half its smallest dimension:
**10 km** (half the 20 km height).

If aircraft A and B are within 8 NM (~14.8 km) of each other:

1. A is somewhere in its geohash-4 cell C_A.
2. B is at most 14.8 km from A.
3. The farthest B can be from the *centre* of C_A is:
   `diagonal half-cell + separation = √(19.5² + 10²) + 14.8 ≈ 37 km`.
4. A 3×3 expansion extends **one full cell width** in every direction from C_A,
   reaching at least 20 km (height) or 39 km (width) beyond the cell boundary.
5. 37 km < 39 km (width) and < 3×20 km = 60 km (height coverage), so B is
   always inside at least one of the 9 expanded cells.

The argument works for all bearings because the expansion is square, not radial.
At diagonal corners the coverage is ~55 km, which still safely exceeds 14.8 km.

---

### Q13. The database stores geohash at precision 5 rather than precision 4. Why the difference?

**Answer:**
The two precisions serve different purposes:

| Use | Precision | Cell size | Reason |
|---|---|---|---|
| Candidate generation (Spark join) | 4 | ~20 km | Large cells → fewer total cells → cheaper self-join lookup |
| Database storage / PostGIS indexing | 5 | ~4.9 km | Finer cells → tighter spatial grouping → better GIST index selectivity |

The GIST index on `geohash5` in `separation_events` lets queries like "find all
events in this geohash-5 cell" scan a tiny fraction of the table.  At precision
4 each cell contains ~16× more events on average, making the index far less
selective.

Storing precision 5 also enables geohash-based aggregation in analytical
queries ("count events per 5 km cell") without calling PostGIS geometry
functions, which is significantly faster.

---

### Q14. How does the pure-Python geohash fallback in `geohash_partitioner.py` work, and why does the C extension matter for performance?

**Answer:**
The module attempts `import geohash` (the `python-geohash` C extension) and
falls back to a pure-Python implementation if it's unavailable:

```python
try:
    import geohash as _gh
    def encode(lat, lon, precision): return _gh.encode(lat, lon, precision)
    def decode_bbox(code): return _gh.bbox(code)
except ImportError:
    # pure-Python base-32 implementation
    ...
```

The pure-Python encoder iterates through bits manually, performing bit
manipulation and list indexing in a Python loop — typically ~5 µs per call at
precision 4.  The C extension does the same work in compiled C — ~1 µs per
call.

`gh_expand` calls `gh_encode` nine times (centre + 8 neighbours), so the
difference compounds: ~45 µs (pure-Python) vs ~9 µs (C extension) per aircraft
per batch.  For 500 aircraft that's 500 × 45 µs = 22.5 ms just for geohash
encoding — a meaningful fraction of the 10-second batch budget.

Installing `python-geohash` (`pip install python-geohash`) cuts geohash time
by 3–5× with no code changes.

---

## 4. PostGIS & Database Design

### Q15. Walk me through the `separation_events` schema and explain why each column exists.

**Answer:**

```sql
CREATE TABLE separation_events (
    id              BIGSERIAL PRIMARY KEY,
    icao24_a        TEXT NOT NULL,
    icao24_b        TEXT NOT NULL,
    callsign_a      TEXT,
    callsign_b      TEXT,
    observed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    horizontal_nm   DOUBLE PRECISION NOT NULL,
    vertical_ft     DOUBLE PRECISION,
    closure_rate_kt DOUBLE PRECISION,
    severity        TEXT NOT NULL,
    pattern         TEXT,
    altitude_a_ft   DOUBLE PRECISION,
    altitude_b_ft   DOUBLE PRECISION,
    geom_a          GEOMETRY(Point, 4326),
    geom_b          GEOMETRY(Point, 4326),
    midpoint        GEOMETRY(Point, 4326),
    geohash5        TEXT
);
```

| Column | Rationale |
|---|---|
| `icao24_a/b` | Aircraft identifiers for joins, dedup, pattern analysis |
| `callsign_a/b` | Human-readable flight IDs; nullable because not all aircraft broadcast callsign |
| `observed_at TIMESTAMPTZ` | Timezone-aware timestamps essential for time-window queries (UTC) |
| `horizontal_nm`, `vertical_ft` | Raw separation values for threshold re-evaluation without recomputing |
| `closure_rate_kt` | Rate of approach; positive = converging, negative = diverging |
| `severity` | Enum (`PROXIMITY`, `NEAR_MISS`, `LOSS_OF_SEPARATION`) for fast filter |
| `pattern` | Known pattern tag (parallel ILS, formation flight) to suppress false alerts |
| `altitude_a_ft`, `altitude_b_ft` | Individual altitudes for 3D replay |
| `geom_a`, `geom_b` | Individual positions for PostGIS spatial queries |
| `midpoint` | Pre-computed midpoint geometry for HDBSCAN clustering and map display |
| `geohash5` | Precision-5 cell for fast equality-join clustering without ST_Within |

`vertical_ft` is nullable because some aircraft don't broadcast altitude —
the system classifies these as `UNKNOWN` rather than discarding them.

---

### Q16. Why use a GIST index on geometry columns rather than a B-tree index?

**Answer:**
B-tree indexes work on linearly ordered data: integers, timestamps, strings.
Spatial data is 2-dimensional — there is no single linear ordering of (lat, lon)
pairs that preserves proximity.  A B-tree on longitude alone can't efficiently
answer "find all points within 10 km of this location" because points nearby
in space can be far apart in any 1D ordering.

**GIST (Generalized Search Tree)** is a framework for multi-dimensional
balanced tree structures.  PostGIS implements an R-tree variant using GIST:
each node stores a bounding box that encloses all child geometries.  A spatial
query like `ST_DWithin(geom, point, radius)` prunes entire branches where the
bounding box doesn't intersect the search radius — typically scanning <1% of
rows for point-in-polygon or distance queries.

```sql
CREATE INDEX idx_sep_events_midpoint ON separation_events USING GIST (midpoint);
```

This index makes the HDBSCAN clustering query (which reads all events from the
last N days, filtered by geography) fast even with millions of rows.

---

### Q17. What does the `cleanup_old_tracks()` function in `init.sql` do, and how would you call it in production?

**Answer:**
`cleanup_old_tracks()` deletes rows from `aircraft_tracks` older than a
configurable retention period (default 24 hours):

```sql
CREATE OR REPLACE FUNCTION cleanup_old_tracks(retention_hours INT DEFAULT 24)
RETURNS void AS $$
BEGIN
    DELETE FROM aircraft_tracks WHERE observed_at < now() - (retention_hours || ' hours')::INTERVAL;
END;
$$ LANGUAGE plpgsql;
```

`aircraft_tracks` stores sampled aircraft positions for replay and trajectory
visualization.  Without periodic cleanup, the table grows unboundedly.

**Production invocation options:**

1. **pg_cron extension**: `SELECT cron.schedule('0 * * * *', 'SELECT cleanup_old_tracks(24)');`
   — runs hourly inside PostgreSQL, no external scheduler needed.
2. **Application-side timer**: FastAPI background task calls it alongside the
   HDBSCAN clustering loop.
3. **PostgreSQL partitioning**: partition `aircraft_tracks` by day (range
   partition on `observed_at`) and use `DROP TABLE` on old partitions — much
   faster than row-by-row deletion for high-volume tables.

---

### Q18. Why does the hotspot clustering script use `DELETE FROM hotspots; INSERT INTO hotspots ...` rather than an UPSERT?

**Answer:**
HDBSCAN recomputes the *entire* cluster set from scratch every 60 seconds.
Cluster IDs are not stable across runs — a new event might merge two clusters
or split one.  There is no meaningful "cluster A from run 1 corresponds to
cluster A from run 2" mapping.

An UPSERT (`INSERT ... ON CONFLICT DO UPDATE`) requires a stable unique key to
match old and new rows.  Since cluster IDs change between runs, an UPSERT would
silently accumulate stale clusters (old IDs that no longer exist).

The `DELETE; INSERT` pattern (executed inside a single transaction for
atomicity) replaces the entire result set atomically:

```python
async with conn.transaction():
    await conn.execute("DELETE FROM hotspots")
    await conn.executemany("INSERT INTO hotspots ...", rows)
```

The API's concurrent reads see either the old complete set or the new complete
set — never a partial state.  The trade-off is a brief table-lock during the
transaction, which is acceptable given the 60-second recompute interval and
the small table size (typically < 100 clusters).

---

## 5. Redis — Caching & Pub/Sub

### Q19. Why store live aircraft positions in Redis rather than PostgreSQL?

**Answer:**
Live position data has very different access patterns from event data:

| Characteristic | Aircraft positions | Separation events |
|---|---|---|
| Write rate | ~50 writes/s (one per aircraft per 10 s batch) | ~1–10 writes/s (events only) |
| Read rate | ~10 reads/s (10-second map poll × all clients) | ~0.03 reads/s (30-second map poll) |
| TTL | 60 seconds (stale = irrelevant) | Permanent (historical record) |
| Query type | Key lookup by `icao24` | Spatial/time-range SQL queries |
| Size | ~200 bytes JSON per aircraft | ~500 bytes per event |

Redis is optimised for high-throughput key-value operations with O(1) GET/SET
latency (~100 µs vs ~1 ms for a PostgreSQL round-trip).  The 60-second TTL
acts as automatic garbage collection — aircraft that stop transmitting simply
expire from Redis without any cleanup job.

PostgreSQL `aircraft_tracks` is still written for replay and trajectory
analysis, but the live map reads from Redis for latency reasons.

---

### Q20. Explain the Redis pub/sub mechanism used for WebSocket alert delivery.

**Answer:**
The flow has three components:

1. **Publisher (Spark job)**: After classifying a `LOSS_OF_SEPARATION` or
   `NEAR_MISS` event, publishes a JSON message to the `adsb:alerts` Redis
   channel:
   ```python
   redis_client.publish("adsb:alerts", json.dumps(alert_dict))
   ```

2. **Subscriber/Fan-out (FastAPI WebSocket endpoint)**: On startup, the API
   subscribes to `adsb:alerts`.  When a message arrives, it broadcasts to all
   connected WebSocket clients:
   ```python
   async for message in pubsub.listen():
       for ws in connected_clients:
           await ws.send_text(message["data"])
   ```

3. **Consumer (Browser)**: The frontend opens a WebSocket to `/ws/alerts` and
   appends each JSON alert to the live alert feed panel.

**Why pub/sub instead of the API polling Redis?**
Polling would add latency (poll interval) and waste CPU on empty polls.
Pub/sub delivers the message within milliseconds of the Spark publish call,
giving near-real-time alerts with no polling overhead.  The `adsb:alerts`
channel can have multiple API subscribers (horizontal scaling) without
duplication — each subscriber gets every message independently.

---

### Q21. What is the `alert:{icao24_a}:{icao24_b}` key and why does it have a 5-minute TTL?

**Answer:**
When a near-miss or LoS event is detected, the Spark job writes a secondary
Redis key `alert:{icao24_a}:{icao24_b}` with a 5-minute TTL in addition to
publishing to the pub/sub channel.

**Purpose — alert deduplication:**
Two aircraft in a sustained near-miss situation (e.g., flying parallel routes
that are just inside the threshold) would generate a new event every 10-second
micro-batch.  Without deduplication, the WebSocket alert feed would flood with
identical alerts.

The API can check `EXISTS alert:{a}:{b}` before forwarding a pub/sub message to
WebSocket clients.  If the key exists, the alert was already delivered in the
last 5 minutes — skip it.  This reduces alert fatigue without missing genuinely
new events (after 5 minutes the key expires and the next event is forwarded).

**5-minute TTL rationale:**
Longer than a single micro-batch (10 s) to suppress sustained proximity events,
but short enough that a re-encounter after separation is treated as a new event.

---

## 6. HDBSCAN Hotspot Clustering

### Q22. Why was HDBSCAN chosen over DBSCAN for hotspot detection?

**Answer:**
Both algorithms are density-based, but they differ in how they handle
non-uniform density — which is the defining characteristic of airspace:

| Scenario | DBSCAN (global epsilon) | HDBSCAN (hierarchical) |
|---|---|---|
| Airport TMA (dense events) | Works well if epsilon is tuned to local density | Works out of the box |
| En-route airspace (sparse events) | Misses clusters because global epsilon is too tight | Detects sparse clusters at a different density level |
| Mixed density (both) | Must choose one epsilon — either misses sparse clusters or merges dense ones | Extracts clusters at appropriate density levels simultaneously |

DBSCAN requires a global `epsilon` (maximum distance between points in a
cluster).  A single value can't capture both tight airport clusters and
diffuse en-route hotspots simultaneously.

HDBSCAN builds a complete cluster hierarchy at all density levels, then extracts
the most persistent clusters using the "excess of mass" (`eom`) selection method.
The `min_cluster_size=3` parameter (minimum 3 events to form a hotspot) controls
minimum cluster size without a distance threshold.

The implementation also uses `metric="haversine"` so cluster membership is based
on great-circle distance rather than Euclidean distance (which would distort
distances at higher latitudes).

---

### Q23. How is the convex hull geometry derived and what happens when all events in a cluster are at the same location?

**Answer:**
After HDBSCAN assigns cluster labels, the geometry for each cluster is computed
using Shapely:

```python
from shapely.geometry import MultiPoint

points = MultiPoint([(lon, lat) for lat, lon in cluster_coords])
hull = points.convex_hull
```

`convex_hull` returns:
- A **Point** if all coordinates are identical (degenerate case).
- A **LineString** if all points are collinear.
- A **Polygon** for 3+ non-collinear points.

**Degenerate case handling:**
Events from a physics-bug era where aircraft barely moved produced clusters
where all events were at the same coordinate.  A Point geometry at zoom level 4
is invisible on the map.

The fix:
```python
if hull.geom_type == "Point":
    hull = hull.buffer(0.3)      # ~25 km radius polygon
elif hull.geom_type == "LineString":
    hull = hull.buffer(0.15)     # ~12 km wide corridor
```

`buffer(0.3)` in EPSG:4326 (degree units) adds ~0.3° ≈ 33 km at the equator,
≈ 25 km at 40°N.  This is a map-visibility hack, not a geographic accuracy
claim — the polygon simply indicates "events occurred in this general area".

For production you would use a projected CRS (e.g., EPSG:3857 Web Mercator)
and buffer in metres: `hull.buffer(25_000)` for a precise 25 km radius.

---

## 7. FastAPI & asyncpg

### Q24. What is the asyncpg JSON codec bug that caused all near-miss events to appear at coordinates [0, 0]?

**Answer:**
PostgreSQL `ST_AsGeoJSON(geom)::json` casts the GeoJSON text output to the
`json` type.  asyncpg ≥ 0.24 changed how it handles the `json` PostgreSQL type:
it now returns the raw JSON **string** to Python rather than a decoded `dict`.

The API route reads the geometry column with:
```python
row["midpoint"]   # asyncpg returns '{"type":"Point","coordinates":[-98.35,39.5]}'
                  # instead of   {"type": "Point", "coordinates": [-98.35, 39.5]}
```

The FastAPI response serializer then JSON-encodes the string again:
```json
{"midpoint": "{\"type\":\"Point\",\"coordinates\":[-98.35,39.5]}"}
```

The JavaScript frontend receives `midpoint` as a string, not an object.
`d.midpoint?.coordinates` is `undefined`, so deck.gl's ScatterplotLayer falls
back to `[0, 0]` — the Gulf of Guinea — for every event.

**Fix:** Register a type codec in the asyncpg connection pool's `init` callback:

```python
async def _init_asyncpg_conn(conn):
    await conn.set_type_codec(
        "json",
        schema="pg_catalog",
        encoder=json.dumps,
        decoder=json.loads,
    )

pool = await asyncpg.create_pool(DATABASE_URL, init=_init_asyncpg_conn)
```

Now every `json`-typed column is automatically decoded to a Python `dict` on
read and encoded from a `dict` on write, system-wide without changing any route
code.

---

### Q25. How do the FastAPI tests avoid needing a live PostgreSQL or Redis instance?

**Answer:**
The test suite uses `httpx.AsyncClient` with `ASGITransport`:

```python
async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
    yield c
```

`ASGITransport` dispatches HTTP requests directly to the ASGI app in-process,
bypassing the network stack entirely.  Crucially, it does **not** send ASGI
lifespan events (`startup`/`shutdown`), so FastAPI's `lifespan` context manager
— which creates the asyncpg pool and Redis client — is never called.

This means `app.state.db` and `app.state.redis` are `None` at test start.
Each test injects `AsyncMock` objects directly:

```python
app.state.db = AsyncMock()
app.state.db.fetch.return_value = [Record({...})]
```

`AsyncMock` from `unittest.mock` returns awaitable coroutines by default, so
`await app.state.db.fetch(sql, *args)` works without a real database.

The `Record(dict)` helper class mimics asyncpg's Record type — routes call
`dict(record)` or `record["field"]`, both of which work because `Record`
subclasses `dict`.

This approach is:
- **Fast**: no I/O, no container startup.
- **Isolated**: each test controls exactly what the DB/Redis returns.
- **Deterministic**: no timing or network flakiness.

---

### Q26. Describe the FastAPI lifespan pattern and what resources it manages.

**Answer:**
FastAPI's `lifespan` context manager (introduced in Starlette 0.20) replaces
the deprecated `@app.on_event("startup")` / `@app.on_event("shutdown")`
decorators.  It manages the full lifecycle of shared resources:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # === STARTUP ===
    app.state.db = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, init=_init_asyncpg_conn
    )
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    task = asyncio.create_task(_clustering_loop())

    yield   # ← application serves requests here

    # === SHUTDOWN ===
    task.cancel()
    await app.state.db.close()
    await app.state.redis.aclose()

app = FastAPI(lifespan=lifespan)
```

Resources managed:
1. **asyncpg connection pool**: reuses TCP connections across requests, avoids
   per-request connection handshake overhead.
2. **aioredis client**: single async Redis connection shared across requests.
3. **HDBSCAN background task**: `asyncio.create_task(_clustering_loop())` runs
   concurrently with request handling; cancelled on shutdown to avoid dangling
   coroutines.

The `yield` statement is the key: code before it runs at startup, code after it
runs at shutdown (even if an exception occurred).  This guarantees connection
pools are always closed cleanly.

---

## 8. System Design & Architecture Trade-offs

### Q27. This system uses three different data stores (Kafka, Redis, PostgreSQL). Justify the polyglot persistence approach.

**Answer:**
Each store is chosen for its fit to a specific access pattern:

| Store | Role | Why it fits |
|---|---|---|
| **Kafka** | Durable message queue between ingestion and processing | Decouples producers from consumers; replay on pipeline failure; handles burst traffic by buffering |
| **Redis** | Live aircraft state cache + alert pub/sub | Sub-millisecond key lookup; automatic TTL expiry; native pub/sub without polling |
| **PostgreSQL + PostGIS** | Historical event store + spatial queries + clustering source | ACID guarantees; complex SQL time/spatial queries; GIST indexes for geospatial lookups |

A single-store alternative:
- **PostgreSQL only**: writing every aircraft ping to PostgreSQL at 50 writes/s
  is viable, but LISTEN/NOTIFY is less capable than Redis pub/sub, and live
  reads would compete with heavy write load.
- **Redis only**: no persistence, no SQL aggregations, no spatial indexing.
- **Kafka only**: no random-access reads, no SQL.

The three stores serve genuinely different roles with minimal overlap.  The
operational cost (three running services) is offset by each component excelling
at its purpose.

---

### Q28. The geohash candidate generation grows as O(n²) with fleet size. How would you scale the pipeline to handle 50,000 aircraft (e.g., global ADS-B)?

**Answer:**
At 50,000 aircraft the naive self-join would produce ~1.25 billion candidate
pairs — far too many for a 10-second batch.  Scaling strategies:

1. **Distribute the self-join across Spark executors:**
   Partition the exploded DataFrame by geohash cell before the self-join.
   Each executor handles only the pairs within its assigned cells.  This
   distributes the O(n²) work across N executors.

2. **Increase geohash precision:**
   At precision 5 (~5 km cells), 50,000 aircraft spread globally produce far
   fewer same-cell collisions than precision 4.  The 3×3 expansion still
   guarantees coverage, and each cell contains far fewer candidates.

3. **Separate high-density airspaces:**
   Route traffic from major TMAs (KORD, KLAX, EGLL) to dedicated streaming
   pipelines with tighter windows.  En-route traffic uses a coarser pipeline
   with less frequent checks.

4. **Vectorised Haversine (NumPy/Cython):**
   Replace Python-loop haversine with `numpy` broadcasting — 100–1000× faster
   for large arrays of candidate pairs.

5. **Drop `toPandas()`, use Spark SQL or Arrow:**
   `toPandas()` serialises the full DataFrame to the driver.  Use Spark's
   vectorised pandas UDFs (`mapInPandas`) to keep computation distributed.

6. **Reduce micro-batch frequency:**
   Increase from 10 s to 30 s for en-route separation (ICAO allows 5 NM at
   FL290+ where aircraft move ~8 NM/min, so 30 s is still safe).

---

### Q29. What are the failure modes of this architecture and how would you make it production-hardened?

**Answer:**

| Component | Failure mode | Mitigation |
|---|---|---|
| Kafka broker | Broker crash → data loss | 3-node KRaft cluster, `replication.factor=3`, `acks=all` |
| Spark job | OOM or exception → batch failure | Checkpointing restarts from last committed offset; set `maxOffsetsPerTrigger` to cap batch size |
| PostgreSQL | Disk full → insert failures | Automated `pg_dump` backups; `aircraft_tracks` partitioning + old partition drops; storage alerts |
| Redis | Crash → live data lost | Redis Sentinel or Redis Cluster for HA; `appendonly yes` for persistence (acceptable since TTL = 60 s, some loss is OK) |
| FastAPI | Process crash → WebSocket clients disconnected | Deploy behind gunicorn with multiple Uvicorn workers; sticky sessions for WebSocket |
| asyncpg pool | Pool exhausted under load | Tune `max_size`; add circuit breaker; return 503 if pool wait exceeds timeout |
| HDBSCAN task | sklearn exception → stale hotspots | Try/except with exponential backoff; expose last_clustered_at in `/api/stats` |
| Network partition | Kafka/Spark can't reach each other | Spark's `failOnDataLoss=false` to tolerate temporary offset gaps; alerts on consumer lag |

Additional hardening:
- **Structured logging** (JSON to stdout) for log aggregation.
- **Prometheus metrics** via `prometheus-fastapi-instrumentator` for API
  latency, Kafka consumer lag, Spark batch duration.
- **Health checks** on all services (already in `docker-compose.yml`).
- **Dead Letter Queue monitoring** — alert if DLQ message rate exceeds threshold.

---

## 9. Testing & Observability

### Q30. How would you add end-to-end integration tests that actually connect to Kafka and PostgreSQL?

**Answer:**
The current test suite uses mocks for speed and isolation.  Integration tests
require real services — typically via `docker-compose` or `testcontainers`:

```python
# Using testcontainers-python
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def kafka():
    with KafkaContainer("confluentinc/cp-kafka:7.5") as k:
        yield k.get_bootstrap_server()

@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgis/postgis:16-3.4") as pg:
        pg.get_connection_url()  # apply init.sql schema
        yield pg
```

Integration test structure:
1. Produce N synthetic ADS-B messages to Kafka.
2. Run `run_batch(pdf)` (the Spark batch function) directly with the test
   messages — no actual Spark cluster needed.
3. Assert that `separation_events` in PostgreSQL contains the expected events.
4. Assert that the expected Redis keys were written with correct TTLs.
5. Call the FastAPI endpoints via `httpx.AsyncClient` with a real asyncpg pool
   and assert GeoJSON responses have correct geometry.

Tests marked `@pytest.mark.integration` can be skipped in CI unless the
`--integration` flag is passed, keeping the unit test suite fast.

---

### Q31. What metrics would you expose to monitor the health of this streaming pipeline in production?

**Answer:**

**Kafka metrics (via JMX or Kafka Exporter):**
- `kafka_consumer_group_lag` per partition — rising lag means Spark can't keep up.
- `kafka_topic_messages_in_total` — drop indicates upstream ingestion failure.
- `adsb.dlq` message rate — spike indicates upstream schema change.

**Spark metrics (Spark UI / Prometheus sink):**
- `spark.streaming.lastCompletedBatch.processingDelay` — should be < 10 s.
- `spark.streaming.lastCompletedBatch.schedulingDelay` — rising delay means
  batches are queuing behind each other.
- Batch input rows per second — drop indicates Kafka consumer falling behind.

**FastAPI metrics (prometheus-fastapi-instrumentator):**
- `http_request_duration_seconds{endpoint="/api/live-traffic"}` P95.
- `websocket_connections_active` — connected clients.
- `http_requests_total{status_code="500"}` — application errors.

**Custom application metrics:**
- Events detected per micro-batch (by severity).
- Redis `DBSIZE` and memory usage.
- `aircraft_tracks` row count growth rate.
- HDBSCAN clustering duration and cluster count.
- asyncpg pool `size`, `free_size` — pool exhaustion signals.

**Alerting thresholds:**
- Consumer lag > 5 batches → page on-call.
- No LoS events in 10 minutes during simulation → pipeline stalled.
- P95 API latency > 500 ms → investigate connection pool.

---

## 10. Performance & Scalability

### Q32. The benchmark shows `gh_expand` at 49 µs P50, dominating the latency profile. What are the optimisation options?

**Answer:**
`gh_expand` computes the 9-cell neighbourhood by:
1. Calling `gh_encode` 8 times (N/S/E/W/NE/NW/SE/SW neighbours).
2. Returning a list of 9 geohash strings.

**Optimisation options, ordered by implementation effort:**

1. **Install the C extension** (`pip install python-geohash`):
   Reduces `gh_encode` from ~5 µs to ~1 µs; `gh_expand` from ~49 µs to ~9 µs.
   Zero code changes.

2. **Pre-compute neighbourhoods once per batch:**
   In `run_batch`, `gh_expand(gh_encode(lat, lon, 4))` is called once per
   aircraft.  Results are already computed once and cached in the `cells` column
   — no per-pair overhead.  The current implementation already does this.

3. **Cache recent expansions with `functools.lru_cache`:**
   Aircraft positions change slowly (≤10 s between batches), so the same
   geohash-4 cell may appear in many consecutive batches.  An LRU cache on
   `gh_expand` with `maxsize=512` can achieve high hit rates for steady-state
   traffic with minimal memory cost.

4. **Use geohash integer encoding:**
   Encode as a 64-bit integer (bit-interleaved lat/lon) rather than a
   base-32 string.  Neighbour computation becomes bit manipulation — no string
   allocation, cache-friendly.

5. **Vectorised geohash with NumPy:**
   Implement the bit-interleave encoder in NumPy, processing all aircraft
   latitudes/longitudes as arrays in one pass.  Eliminates Python loop overhead
   for the geohash step entirely.

---

### Q33. The throughput benchmark shows O(n²) pair growth. At what fleet size does the pipeline become infeasible, and what is the solution?

**Answer:**
From the benchmark data:

| Fleet | Pairs | Time (ms) | Batches/s |
|---|---|---|---|
| 50 | 283 | 14 | 70 |
| 100 | 1,027 | 35 | 29 |
| 200 | 3,841 | 104 | 10 |
| 500 | 17,204 | 617 | 1.6 |

The pairs grow as `O(n²)` because dense airspace means more same-cell
collisions after geohash expansion.  Extrapolating:

- Fleet 1,000: ~70,000 pairs, ~2,500 ms → infeasible for a 10-second budget.
- Fleet 2,000: ~280,000 pairs, ~10,000 ms → completely infeasible.

The inflection point is around **700–800 aircraft** where the batch duration
approaches 10 seconds.

**Solutions:**

1. **Geographic partitioning**: Split the continental US into 4–6 regions
   (ARTCC boundaries match real ATC sectors).  Run one Spark streaming query
   per region; each sees only ~100–150 aircraft → back to feasible.

2. **Coarser geohash + tighter filter**: Use precision 3 cells for candidate
   generation but immediately filter to `horiz_nm < 10 NM` with a spatial
   index before the haversine pass.  Reduces the set of pairs evaluated by
   haversine.

3. **Approximate nearest neighbours**: Use a spatial index (KD-tree from
   `scipy.spatial`) to find all pairs within 8 NM directly, bypassing the
   geohash self-join altogether.  `cKDTree.query_ball_tree` scales as
   `O(n log n)` for typical spatial distributions.

4. **Distributed Spark with shuffle-based join**: A proper Spark cluster can
   distribute the self-join across N executors, reducing per-executor work to
   ~O((n/N)²).  With 8 executors, fleet=2000 becomes fleet=250 per executor.

---

### Q34. Why does the benchmark report `Proximity = 0` for random fleet traffic, and what does this tell you about the geohash candidate filter effectiveness?

**Answer:**
The continental US spans approximately 8 million km².  With 500 aircraft
randomly distributed, the average nearest-neighbour distance is:

```
d ≈ √(8,000,000 km² / 500) ≈ 126 km ≈ 68 NM
```

The proximity threshold is 8 NM (~15 km).  For randomly placed aircraft, the
probability that any pair is within 8 NM is vanishingly small — hence
`Proximity = 0` in all benchmark runs.

This tells you the geohash candidate filter is highly effective:

- **17,204 candidate pairs** are generated for a 500-aircraft fleet after
  geohash expansion and self-join.
- **0 proximity events** survive the haversine + altitude filter.
- The filter selectivity is **17,204 / C(500,2) ≈ 17,204 / 124,750 ≈ 13.8%**:
  the geohash step eliminates 86% of all possible pairs before the expensive
  haversine computation.

In real-world dense airspace (e.g., KJFK approach corridor with 200 aircraft
in a 60×60 km box), the geohash filter would be far less effective because
many pairs share cells.  This is exactly when geographic partitioning and
vectorised haversine become necessary.

Near-miss events only appear in the benchmarks when the simulator injects
converging pairs — the demo mode's `simulate-small` target does exactly this to
validate the end-to-end pipeline.

---

*These questions cover the core data engineering concepts present in the
ADS-B Near-Miss Detector codebase.  Interviewers may ask follow-ups like "how
would you handle X at 10× scale?" or "what would you do differently?".  The
best answers demonstrate understanding of the trade-offs, not just the
implementation details.*
