from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


# ── GeoJSON primitives ────────────────────────────────────────────────────────

class GeoJSONPoint(BaseModel):
    type: str = "Point"
    coordinates: list[float]


class GeoJSONFeature(BaseModel):
    type: str = "Feature"
    geometry: dict[str, Any]
    properties: dict[str, Any]


class GeoJSONFeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: list[GeoJSONFeature]


# ── Separation events ─────────────────────────────────────────────────────────

class SeparationEvent(BaseModel):
    id: int
    icao24_a: str
    icao24_b: str
    callsign_a: Optional[str]
    callsign_b: Optional[str]
    midpoint: dict[str, Any]
    geom_a: dict[str, Any]
    geom_b: dict[str, Any]
    horizontal_nm: float
    vertical_ft: float
    closure_rate_kt: Optional[float]
    severity: str
    pattern: Optional[str]
    altitude_a_ft: Optional[float]
    altitude_b_ft: Optional[float]
    observed_at: datetime


class SeparationEventDetail(SeparationEvent):
    """Single event with surrounding aircraft tracks for both planes."""
    tracks_a: list[dict[str, Any]] = []
    tracks_b: list[dict[str, Any]] = []


class NearMissesResponse(BaseModel):
    total: int
    events: list[SeparationEvent]


# ── Hotspots ──────────────────────────────────────────────────────────────────

class HotspotProperties(BaseModel):
    id: int
    event_count: int
    avg_severity: Optional[float]
    dominant_pattern: Optional[str]
    airspace_class: Optional[str]
    nearest_airport: Optional[str]
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]


class HotspotEventsResponse(BaseModel):
    hotspot_id: int
    total: int
    events: list[dict[str, Any]]


# ── Stats ─────────────────────────────────────────────────────────────────────

class StatsResponse(BaseModel):
    events_last_hour: int
    events_last_24h: int
    live_aircraft_count: int
    severity_breakdown: dict[str, int]
    top_hotspots: list[dict[str, Any]]
