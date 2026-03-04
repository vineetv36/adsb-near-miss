"""
FastAPI endpoint tests.

Strategy
--------
We use ``httpx.AsyncClient`` with ``ASGITransport`` which calls the ASGI
app directly *without* triggering the FastAPI lifespan (so no real
PostgreSQL or Redis connection is needed).  Each test injects mock
``app.state.db`` and ``app.state.redis`` objects.

AsyncMock return values are configured per-test so every route
is exercised in isolation.
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock

from src.api.main import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Record(dict):
    """
    Minimal stand-in for an asyncpg Record.
    Routes call ``dict(record)`` or access ``record["field"]``; both work
    because Record subclasses dict.
    """


def make_db(
    *,
    fetch=None,
    fetchval=0,
    fetchrow=None,
) -> AsyncMock:
    db = AsyncMock()
    db.fetch.return_value = fetch or []
    db.fetchval.return_value = fetchval
    db.fetchrow.return_value = fetchrow
    return db


def make_redis(
    *,
    keys=None,
    mget=None,
) -> AsyncMock:
    r = AsyncMock()
    r.keys.return_value = keys or []
    r.mget.return_value = mget or []
    return r


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client():
    """Async HTTP client with default empty mocks; tests override as needed."""
    app.state.db = make_db()
    app.state.redis = make_redis()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# GET /api/live-traffic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_live_traffic_empty(client):
    app.state.redis = make_redis(keys=[])
    r = await client.get("/api/live-traffic")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    assert body["features"] == []


@pytest.mark.asyncio
async def test_live_traffic_single_aircraft(client):
    payload = {
        "icao24": "abc123",
        "callsign": "UAL123",
        "latitude": 39.5,
        "longitude": -98.35,
        "altitude_ft": 35_000.0,
        "velocity_kts": 450.0,
        "true_track": 270.0,
    }
    app.state.redis = make_redis(
        keys=["aircraft:abc123"],
        mget=[json.dumps(payload)],
    )
    r = await client.get("/api/live-traffic")
    assert r.status_code == 200
    body = r.json()
    assert len(body["features"]) == 1
    feat = body["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"] == {"type": "Point", "coordinates": [-98.35, 39.5]}
    props = feat["properties"]
    assert props["icao24"] == "abc123"
    assert props["callsign"] == "UAL123"
    assert "latitude" not in props
    assert "longitude" not in props


@pytest.mark.asyncio
async def test_live_traffic_multiple_aircraft(client):
    aircraft = [
        {"icao24": f"a{i}", "latitude": 40.0 + i, "longitude": -90.0 + i}
        for i in range(3)
    ]
    app.state.redis = make_redis(
        keys=[f"aircraft:a{i}" for i in range(3)],
        mget=[json.dumps(a) for a in aircraft],
    )
    r = await client.get("/api/live-traffic")
    body = r.json()
    assert len(body["features"]) == 3


@pytest.mark.asyncio
async def test_live_traffic_skips_invalid_json(client):
    app.state.redis = make_redis(
        keys=["aircraft:good", "aircraft:bad"],
        mget=[
            json.dumps({"icao24": "good", "latitude": 40.0, "longitude": -75.0}),
            "not-valid-json{{",
        ],
    )
    r = await client.get("/api/live-traffic")
    assert r.status_code == 200
    assert len(r.json()["features"]) == 1


@pytest.mark.asyncio
async def test_live_traffic_skips_missing_coordinates(client):
    app.state.redis = make_redis(
        keys=["aircraft:nolat"],
        mget=[json.dumps({"icao24": "nolat", "altitude_ft": 10_000})],
    )
    r = await client.get("/api/live-traffic")
    assert len(r.json()["features"]) == 0


@pytest.mark.asyncio
async def test_live_traffic_skips_null_redis_value(client):
    app.state.redis = make_redis(
        keys=["aircraft:a"],
        mget=[None],
    )
    r = await client.get("/api/live-traffic")
    assert len(r.json()["features"]) == 0


# ---------------------------------------------------------------------------
# GET /api/near-misses
# ---------------------------------------------------------------------------


def _event_row(**overrides) -> Record:
    defaults = Record({
        "id": 1,
        "icao24_a": "abc123",
        "icao24_b": "def456",
        "callsign_a": "UAL123",
        "callsign_b": "DAL456 ",
        "midpoint": {"type": "Point", "coordinates": [-98.35, 39.5]},
        "geom_a":   {"type": "Point", "coordinates": [-98.40, 39.5]},
        "geom_b":   {"type": "Point", "coordinates": [-98.30, 39.5]},
        "horizontal_nm": 3.2,
        "vertical_ft": 800.0,
        "closure_rate_kt": 450.0,
        "severity": "NEAR_MISS",
        "pattern": None,
        "altitude_a_ft": 35_000.0,
        "altitude_b_ft": 35_800.0,
        "observed_at": "2024-01-01T12:00:00+00:00",
    })
    defaults.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_near_misses_empty(client):
    app.state.db = make_db(fetch=[])
    r = await client.get("/api/near-misses")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["events"] == []


@pytest.mark.asyncio
async def test_near_misses_returns_events(client):
    app.state.db = make_db(fetch=[_event_row()])
    r = await client.get("/api/near-misses?hours=24&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    ev = body["events"][0]
    assert ev["severity"] == "NEAR_MISS"
    assert ev["icao24_a"] == "abc123"
    assert ev["midpoint"] == {"type": "Point", "coordinates": [-98.35, 39.5]}


@pytest.mark.asyncio
async def test_near_misses_geometry_is_dict_not_string(client):
    """Geometry fields must be dicts (GeoJSON objects), not raw JSON strings."""
    app.state.db = make_db(fetch=[_event_row()])
    r = await client.get("/api/near-misses")
    ev = r.json()["events"][0]
    assert isinstance(ev["midpoint"], dict), "midpoint should be a dict"
    assert ev["midpoint"]["type"] == "Point"
    assert isinstance(ev["midpoint"]["coordinates"], list)


@pytest.mark.asyncio
async def test_near_misses_invalid_severity_422(client):
    r = await client.get("/api/near-misses?severity=BOGUS")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_near_misses_severity_filter_accepted(client):
    for sev in ("PROXIMITY", "NEAR_MISS", "LOSS_OF_SEPARATION"):
        app.state.db = make_db(fetch=[])
        r = await client.get(f"/api/near-misses?severity={sev}")
        assert r.status_code == 200, f"severity={sev!r} should be accepted"


@pytest.mark.asyncio
async def test_near_misses_default_limit(client):
    """Default limit parameter is passed down to the DB query (mock captures it)."""
    app.state.db = make_db(fetch=[])
    r = await client.get("/api/near-misses")
    assert r.status_code == 200
    app.state.db.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_near_misses_multiple_events(client):
    rows = [_event_row(id=i, severity="PROXIMITY") for i in range(5)]
    app.state.db = make_db(fetch=rows)
    r = await client.get("/api/near-misses")
    body = r.json()
    assert body["total"] == 5
    assert len(body["events"]) == 5


# ---------------------------------------------------------------------------
# GET /api/hotspots
# ---------------------------------------------------------------------------


def _hotspot_row(**overrides) -> Record:
    defaults = Record({
        "id": 1,
        "geom": {
            "type": "Polygon",
            "coordinates": [[
                [-99.0, 38.0], [-97.0, 38.0], [-97.0, 40.0],
                [-99.0, 40.0], [-99.0, 38.0],
            ]],
        },
        "centroid": {"type": "Point", "coordinates": [-98.0, 39.0]},
        "event_count": 15,
        "avg_severity": 1.8,
        "dominant_pattern": None,
        "airspace_class": "E",
        "nearest_airport": "KICT",
        "first_seen": "2024-01-01T10:00:00+00:00",
        "last_seen": "2024-01-01T12:00:00+00:00",
    })
    defaults.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_hotspots_empty(client):
    app.state.db = make_db(fetch=[])
    r = await client.get("/api/hotspots")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    assert body["features"] == []


@pytest.mark.asyncio
async def test_hotspots_returns_feature_collection(client):
    app.state.db = make_db(fetch=[_hotspot_row()])
    r = await client.get("/api/hotspots?min_events=1")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 1


@pytest.mark.asyncio
async def test_hotspots_feature_structure(client):
    app.state.db = make_db(fetch=[_hotspot_row()])
    r = await client.get("/api/hotspots")
    feat = r.json()["features"][0]
    assert feat["type"] == "Feature"
    # Geometry must be a dict (not a string)
    assert isinstance(feat["geometry"], dict)
    assert feat["geometry"]["type"] == "Polygon"
    # Properties
    props = feat["properties"]
    assert props["event_count"] == 15
    assert props["nearest_airport"] == "KICT"
    assert props["airspace_class"] == "E"
    # geom should have been popped into geometry, not duplicated in properties
    assert "geom" not in props


@pytest.mark.asyncio
async def test_hotspots_multiple(client):
    rows = [_hotspot_row(id=i, event_count=10 + i) for i in range(4)]
    app.state.db = make_db(fetch=rows)
    r = await client.get("/api/hotspots")
    assert len(r.json()["features"]) == 4


@pytest.mark.asyncio
async def test_hotspots_min_events_param_passed_to_db(client):
    app.state.db = make_db(fetch=[])
    r = await client.get("/api/hotspots?min_events=5")
    assert r.status_code == 200
    # Verify the DB was queried (min_events is baked into the SQL args)
    app.state.db.fetch.assert_awaited_once()


# ---------------------------------------------------------------------------
# GET /api/stats
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stats_structure(client):
    """Stats endpoint returns all required fields with correct types."""
    db = AsyncMock()
    # asyncio.gather calls: fetchval×2, fetch×2, redis.keys×1
    db.fetchval.side_effect = [42, 150]          # events_last_hour, events_last_24h
    db.fetch.side_effect = [
        [Record({"severity": "NEAR_MISS", "count": 20}),
         Record({"severity": "LOSS_OF_SEPARATION", "count": 5})],
        [],  # top_hotspot_rows
    ]
    app.state.db = db
    app.state.redis = make_redis(keys=["aircraft:a", "aircraft:b"])

    r = await client.get("/api/stats")
    assert r.status_code == 200
    body = r.json()

    assert body["events_last_hour"] == 42
    assert body["events_last_24h"] == 150
    assert body["live_aircraft_count"] == 2
    assert body["severity_breakdown"]["NEAR_MISS"] == 20
    assert body["severity_breakdown"]["LOSS_OF_SEPARATION"] == 5
    assert "top_hotspots" in body


@pytest.mark.asyncio
async def test_stats_empty_db(client):
    db = AsyncMock()
    db.fetchval.side_effect = [0, 0]
    db.fetch.side_effect = [[], []]
    app.state.db = db
    app.state.redis = make_redis(keys=[])

    r = await client.get("/api/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["events_last_hour"] == 0
    assert body["live_aircraft_count"] == 0
    assert body["severity_breakdown"] == {}


@pytest.mark.asyncio
async def test_stats_no_aircraft_in_redis(client):
    db = AsyncMock()
    db.fetchval.side_effect = [10, 80]
    db.fetch.side_effect = [[], []]
    app.state.db = db
    app.state.redis = make_redis(keys=[])

    r = await client.get("/api/stats")
    assert r.json()["live_aircraft_count"] == 0
