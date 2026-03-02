"""GET /api/stats — dashboard summary statistics."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/stats", summary="Dashboard summary statistics")
async def stats(request: Request):
    db = request.app.state.db
    redis = request.app.state.redis

    # Run DB queries and Redis key scan concurrently
    (
        events_last_hour,
        events_last_24h,
        severity_rows,
        top_hotspot_rows,
        aircraft_keys,
    ) = await asyncio.gather(
        db.fetchval(
            "SELECT COUNT(*) FROM separation_events"
            " WHERE observed_at > NOW() - INTERVAL '1 hour'"
        ),
        db.fetchval(
            "SELECT COUNT(*) FROM separation_events"
            " WHERE observed_at > NOW() - INTERVAL '24 hours'"
        ),
        db.fetch(
            """
            SELECT severity, COUNT(*) AS count
            FROM separation_events
            WHERE observed_at > NOW() - INTERVAL '24 hours'
            GROUP BY severity
            """
        ),
        db.fetch(
            """
            SELECT id, ST_AsGeoJSON(centroid)::json AS centroid,
                   event_count, nearest_airport, airspace_class
            FROM hotspots
            ORDER BY event_count DESC
            LIMIT 5
            """
        ),
        redis.keys("aircraft:*"),
    )

    return {
        "events_last_hour": events_last_hour or 0,
        "events_last_24h": events_last_24h or 0,
        "live_aircraft_count": len(aircraft_keys),
        "severity_breakdown": {r["severity"]: r["count"] for r in severity_rows},
        "top_hotspots": [dict(r) for r in top_hotspot_rows],
    }
