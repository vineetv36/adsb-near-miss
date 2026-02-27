-- ============================================================
-- ADS-B Near-Miss Detection – PostgreSQL + PostGIS Schema
-- ============================================================

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- ──────────────────────────────────────────────
-- Raw aircraft positions (sampled, not every ping)
-- ──────────────────────────────────────────────
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

-- ──────────────────────────────────────────────
-- Near-miss / separation events
-- ──────────────────────────────────────────────
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

-- ──────────────────────────────────────────────
-- Hotspot clusters (computed by batch analysis)
-- ──────────────────────────────────────────────
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

-- ──────────────────────────────────────────────
-- Spatial indexes
-- ──────────────────────────────────────────────
CREATE INDEX idx_tracks_geom ON aircraft_tracks USING GIST(geom);
CREATE INDEX idx_tracks_time ON aircraft_tracks(observed_at);
CREATE INDEX idx_tracks_icao24 ON aircraft_tracks(icao24);

CREATE INDEX idx_events_midpoint ON separation_events USING GIST(midpoint);
CREATE INDEX idx_events_time ON separation_events(observed_at);
CREATE INDEX idx_events_severity ON separation_events(severity);

CREATE INDEX idx_hotspots_geom ON hotspots USING GIST(geom);
