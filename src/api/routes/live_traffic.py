"""GET /api/live-traffic — current aircraft positions from Redis as GeoJSON."""

from __future__ import annotations

import json

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/live-traffic", summary="Live aircraft positions (GeoJSON)")
async def live_traffic(request: Request):
    """
    Returns a GeoJSON FeatureCollection of all aircraft currently tracked in
    Redis.  Each key ``aircraft:{icao24}`` holds a JSON object written by the
    Spark streaming job with at minimum ``latitude``, ``longitude``, and
    ``icao24`` fields.
    """
    redis = request.app.state.redis

    keys = await redis.keys("aircraft:*")
    features = []

    if keys:
        values = await redis.mget(*keys)
        for raw in values:
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            lon = data.get("longitude")
            lat = data.get("latitude")
            if lon is None or lat is None:
                continue

            props = {k: v for k, v in data.items() if k not in ("longitude", "latitude")}
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": props,
                }
            )

    return {"type": "FeatureCollection", "features": features}
