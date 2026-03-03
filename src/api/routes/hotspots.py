"""
GET /api/hotspots                  — hotspot polygons as GeoJSON
GET /api/hotspots/{id}/events      — separation events within a hotspot
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


@router.get("/hotspots", summary="Hotspot polygons as GeoJSON FeatureCollection")
async def list_hotspots(
    request: Request,
    min_events: int = Query(3, ge=1, description="Minimum event count to include"),
    days: Optional[int] = Query(
        None, ge=1, description="Only hotspots with activity in the last N days"
    ),
):
    db = request.app.state.db

    where = "WHERE event_count >= $1"
    args: list = [min_events]

    if days is not None:
        args.append(days)
        where += f" AND last_seen > NOW() - make_interval(days => ${len(args)})"

    rows = await db.fetch(
        f"""
        SELECT
            id,
            ST_AsGeoJSON(geom)::json     AS geom,
            ST_AsGeoJSON(centroid)::json AS centroid,
            event_count, avg_severity, dominant_pattern,
            airspace_class, nearest_airport, first_seen, last_seen
        FROM hotspots
        {where}
        ORDER BY event_count DESC
        """,
        *args,
    )

    features = []
    for row in rows:
        r = dict(row)
        geom = r.pop("geom")
        features.append({"type": "Feature", "geometry": geom, "properties": r})

    return {"type": "FeatureCollection", "features": features}


@router.get(
    "/hotspots/{hotspot_id}/events",
    summary="Separation events that fall within a hotspot polygon",
)
async def hotspot_events(
    request: Request,
    hotspot_id: int,
    limit: int = Query(100, ge=1, le=1000),
):
    db = request.app.state.db

    hotspot = await db.fetchrow(
        "SELECT id FROM hotspots WHERE id = $1", hotspot_id
    )
    if not hotspot:
        raise HTTPException(status_code=404, detail="Hotspot not found")

    rows = await db.fetch(
        """
        SELECT
            se.id, se.icao24_a, se.icao24_b, se.callsign_a, se.callsign_b,
            ST_AsGeoJSON(se.midpoint)::json AS midpoint,
            se.horizontal_nm, se.vertical_ft, se.closure_rate_kt,
            se.severity, se.pattern, se.altitude_a_ft, se.altitude_b_ft,
            se.observed_at
        FROM separation_events se
        JOIN hotspots h ON ST_Contains(h.geom, se.midpoint)
        WHERE h.id = $1
        ORDER BY se.observed_at DESC
        LIMIT $2
        """,
        hotspot_id,
        limit,
    )

    events = [dict(r) for r in rows]
    return {"hotspot_id": hotspot_id, "total": len(events), "events": events}
