"""
ADS-B state vector schema definition.

The JSON structure matches the OpenSky Network REST API format so the
simulator and live producer share the same downstream schema.

Field units (OpenSky convention):
  - altitudes   : metres  (baro_altitude, geo_altitude)
  - velocity    : m/s
  - vertical_rate: m/s  (positive = climbing)
  - true_track  : degrees, 0 = North, clockwise
"""

from typing import Optional

# ---------------------------------------------------------------------------
# JSON-Schema / Avro-compatible field spec (informational — used in docs)
# ---------------------------------------------------------------------------
AIRCRAFT_STATE_SCHEMA: dict = {
    "type": "record",
    "name": "AircraftState",
    "namespace": "com.adsb_near_miss.ingestion",
    "doc": "Raw ADS-B state vector — matches OpenSky Network /api/states/all format",
    "fields": [
        {"name": "icao24",           "type": "string",                       "doc": "6-char hex transponder address"},
        {"name": "callsign",         "type": ["null", "string"],              "default": None},
        {"name": "origin_country",   "type": "string"},
        {"name": "time_position",    "type": ["null", "double"],              "default": None, "doc": "Unix timestamp of last position fix"},
        {"name": "last_contact",     "type": "double",                        "doc": "Unix timestamp of last any contact"},
        {"name": "longitude",        "type": ["null", "double"],              "default": None, "doc": "WGS-84 degrees"},
        {"name": "latitude",         "type": ["null", "double"],              "default": None, "doc": "WGS-84 degrees"},
        {"name": "baro_altitude",    "type": ["null", "double"],              "default": None, "doc": "Barometric altitude, metres"},
        {"name": "on_ground",        "type": "boolean"},
        {"name": "velocity",         "type": ["null", "double"],              "default": None, "doc": "Ground speed, m/s"},
        {"name": "true_track",       "type": ["null", "double"],              "default": None, "doc": "Degrees clockwise from north"},
        {"name": "vertical_rate",    "type": ["null", "double"],              "default": None, "doc": "m/s, positive = climbing"},
        {"name": "geo_altitude",     "type": ["null", "double"],              "default": None, "doc": "GPS geometric altitude, metres"},
        {"name": "squawk",           "type": ["null", "string"],              "default": None, "doc": "4-char octal transponder code"},
        {"name": "geohash",          "type": ["null", "string"],              "default": None, "doc": "Partition key — geohash precision 4 (~40×20 km)"},
        # Simulation-only metadata (null in live production data)
        {"name": "_sim",             "type": "boolean",                       "default": False},
        {"name": "_near_miss_pair",  "type": ["null", "string"],              "default": None, "doc": "ICAO24 of paired near-miss aircraft"},
    ],
}


def make_state_dict(
    *,
    icao24: str,
    callsign: Optional[str],
    origin_country: str,
    time_position: Optional[float],
    last_contact: float,
    longitude: Optional[float],
    latitude: Optional[float],
    baro_altitude: Optional[float],
    on_ground: bool,
    velocity: Optional[float],
    true_track: Optional[float],
    vertical_rate: Optional[float],
    geo_altitude: Optional[float],
    squawk: Optional[str],
    geohash: Optional[str],
    sim: bool = False,
    near_miss_pair: Optional[str] = None,
) -> dict:
    """Build a serialisable state-vector dict ready for Kafka."""
    return {
        "icao24": icao24,
        "callsign": callsign,
        "origin_country": origin_country,
        "time_position": time_position,
        "last_contact": last_contact,
        "longitude": longitude,
        "latitude": latitude,
        "baro_altitude": baro_altitude,
        "on_ground": on_ground,
        "velocity": velocity,
        "true_track": true_track,
        "vertical_rate": vertical_rate,
        "geo_altitude": geo_altitude,
        "squawk": squawk,
        "geohash": geohash,
        "_sim": sim,
        "_near_miss_pair": near_miss_pair,
    }
