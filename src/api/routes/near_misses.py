"""
GET /api/near-misses        — recent separation events (filterable)
GET /api/near-misses/{id}   — single event + surrounding aircraft tracks
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()

# ── helpers ───────────────────────────────────────────────────────────────────

_SELECT_EVENT = """
    SELECT
        id, icao24_a, icao24_b, callsign_a, callsign_b,
        ST_AsGeoJSON(midpoint)::json  AS midpoint,
        ST_AsGeoJSON(geom_a)::json    AS geom_a,
        ST_AsGeoJSON(geom_b)::json    AS geom_b,
        horizontal_nm, vertical_ft, closure_rate_kt,
        severity, pattern, altitude_a_ft, altitude_b_ft, observed_at
    FROM separation_events
"""

_SELECT_TRACK = """
    SELECT
        ST_AsGeoJSON(geom)::json AS geom,
        altitude_ft, velocity_kts, true_track, vertical_rate, observed_at
    FROM aircraft_tracks
    WHERE icao24 = $1
      AND observed_at BETWEEN $2 - INTERVAL '5 minutes'
                          AND $2 + INTERVAL '5 minutes'
    ORDER BY observed_at
"""

VALID_SEVERITIES = {"PROXIMITY", "NEAR_MISS", "LOSS_OF_SEPARATION"}


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/near-misses", summary="List recent separation events")
async def list_near_misses(
    request: Request,
    hours: int = Query(24, ge=1, le=720, description="Look-back window in hours"),
    severity: Optional[str] = Query(
        None,
        description="Filter by severity: PROXIMITY | NEAR_MISS | LOSS_OF_SEPARATION",
    ),
    limit: int = Query(100, ge=1, le=1000),
):
    if severity and severity not in VALID_SEVERITIES:
        raise HTTPException(
            status_code=422,
            detail=f"severity must be one of {sorted(VALID_SEVERITIES)}",
        )

    db = request.app.state.db

    where = "WHERE observed_at > NOW() - make_interval(hours => $1)"
    args: list = [hours]

    if severity:
        args.append(severity)
        where += f" AND severity = ${len(args)}"

    query = f"{_SELECT_EVENT} {where} ORDER BY observed_at DESC LIMIT {limit}"
    rows = await db.fetch(query, *args)
    events = [dict(r) for r in rows]
    return {"total": len(events), "events": events}


@router.get("/near-misses/{event_id}", summary="Single separation event with aircraft tracks")
async def get_near_miss(request: Request, event_id: int):
    db = request.app.state.db

    row = await db.fetchrow(f"{_SELECT_EVENT} WHERE id = $1", event_id)
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")

    event = dict(row)
    observed_at = event["observed_at"]

    tracks_a, tracks_b = await asyncio_gather(
        db.fetch(_SELECT_TRACK, event["icao24_a"], observed_at),
        db.fetch(_SELECT_TRACK, event["icao24_b"], observed_at),
    )
    event["tracks_a"] = [dict(r) for r in tracks_a]
    event["tracks_b"] = [dict(r) for r in tracks_b]
    return event


# ── asyncio helper (avoid top-level import cluttering module namespace) ────────

async def asyncio_gather(*coros):
    import asyncio
    return await asyncio.gather(*coros)
