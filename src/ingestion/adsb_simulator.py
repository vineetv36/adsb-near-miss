#!/usr/bin/env python3
"""
ADS-B Simulator
===============
Generates realistic synthetic ADS-B state vectors and produces them to Kafka.

Aircraft physics:
  - N aircraft (default 500) on great-circle routes between major US airports
  - Altitude profiles: climb → cruise (FL300–FL410) → descend
  - Speed: 380–470 kts at cruise, 280–340 kts during climb/descent
  - Position updates staggered 1–2 s per aircraft (mimics real ADS-B ping rate)
  - Noise: GPS jitter ±50 m, baro/geo offset, missed pings, track/velocity noise

Near-miss injection:
  - Every 10-second window, triggered with probability --near-miss-rate
  - Two aircraft converge from ~15 NM to < 3 NM horizontal / < 800 ft vertical
  - Tagged with _near_miss_pair in the Kafka message for downstream validation

Kafka output:
  - Topic  : adsb.raw
  - Key    : geohash at precision 4 (~40×20 km cells)
  - Value  : JSON matching OpenSky Network state-vector format

Usage:
  python src/ingestion/adsb_simulator.py
  python src/ingestion/adsb_simulator.py --aircraft 100 --near-miss-rate 0.05
  python src/ingestion/adsb_simulator.py --bootstrap localhost:9092 --dry-run
  python src/ingestion/adsb_simulator.py --aircraft 50 --near-miss-rate 0.1 \\
      --bootstrap kafka:9092 --log-interval 5
"""

import argparse
import json
import logging
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("adsb.simulator")

# ---------------------------------------------------------------------------
# Physical / aviation constants
# ---------------------------------------------------------------------------
KTS_TO_MS   = 0.514444          # 1 knot  = 0.514444 m/s
MS_TO_KTS   = 1.0 / KTS_TO_MS
NM_TO_M     = 1852.0            # 1 NM    = 1852 m
FT_TO_M     = 0.3048
M_TO_FT     = 1.0 / FT_TO_M
EARTH_R     = 6_371_000.0       # Earth radius in metres

# Cruise altitude band (metres) — FL300 to FL410
MIN_CRUISE_ALT_M = 9_144.0      # 30 000 ft
MAX_CRUISE_ALT_M = 12_497.0     # 41 000 ft

# Vertical rates (metres/second)
CLIMB_RATE_MS   =  8.5          # ≈ 1 700 ft/min
DESCENT_RATE_MS =  7.1          # ≈ 1 400 ft/min

# Fraction of route used for climb / descent transitions
CLIMB_FRAC   = 0.28             # first 28% of route = climbing
DESCENT_FRAC = 0.28             # last  28% of route = descending

# Near-miss geometry
NM_APPROACH_M   = 15 * NM_TO_M  # each aircraft starts 15 NM from convergence
NM_OVERSHOOT_M  =  5 * NM_TO_M  # flies 5 NM past convergence (ensures crossing)

# Kafka topic
TOPIC_RAW = "adsb.raw"

# ---------------------------------------------------------------------------
# US major airports  (ICAO, lat °N, lon °E, elevation m)
# ---------------------------------------------------------------------------
Airport = Tuple[str, float, float, float]

AIRPORTS: List[Airport] = [
    ("KATL", 33.6367,  -84.4281,  313),
    ("KLAX", 33.9425, -118.4081,   38),
    ("KORD", 41.9742,  -87.9073,  204),
    ("KDFW", 32.8998,  -97.0403,  185),
    ("KDEN", 39.8561, -104.6737, 1655),
    ("KJFK", 40.6413,  -73.7781,    4),
    ("KSFO", 37.6213, -122.3790,    4),
    ("KLAS", 36.0840, -115.1537,  664),
    ("KSEA", 47.4502, -122.3088,  132),
    ("KMIA", 25.7959,  -80.2870,    2),
    ("KBOS", 42.3656,  -71.0096,    6),
    ("KPHL", 39.8744,  -75.2424,   11),
    ("KPHX", 33.4373, -112.0078,  346),
    ("KEWR", 40.6925,  -74.1687,    6),
    ("KIAH", 29.9902,  -95.3368,   29),
    ("KDTW", 42.2124,  -83.3534,  197),
    ("KMSP", 44.8848,  -93.2223,  256),
    ("KCLT", 35.2140,  -80.9431,  228),
    ("KSLC", 40.7884, -111.9778, 1288),
    ("KSAN", 32.7338, -117.1933,    5),
    ("KTPA", 27.9755,  -82.5332,    8),
    ("KPDX", 45.5898, -122.5951,    9),
    ("KMCO", 28.4294,  -81.3089,   29),
    ("KSTL", 38.7487,  -90.3700,  184),
    ("KAUS", 30.1975,  -97.6664,  149),
    ("KBWI", 39.1774,  -76.6684,  146),
    ("KPIT", 40.4915,  -80.2329,  368),
    ("KIND", 39.7173,  -86.2944,  248),
    ("KCLE", 41.4117,  -81.8498,  240),
    ("KMEM", 35.0424,  -89.9767,   99),
]

# Representative airline codes for realistic callsigns
_AIRLINES = ["AAL", "DAL", "UAL", "SWA", "ASA", "JBU", "SKW", "FDX", "UPS", "NKS"]

# US bounding box for near-miss convergence points
_US_LAT = (30.0, 46.0)
_US_LON = (-118.0, -75.0)

# ---------------------------------------------------------------------------
# Geohash encoder  (uses python-geohash if installed, falls back to built-in)
# ---------------------------------------------------------------------------
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_encode(lat: float, lon: float, precision: int = 4) -> str:
    """Encode (lat, lon) as a geohash string of given precision."""
    try:
        import geohash as _gh  # python-geohash package
        return _gh.encode(lat, lon, precision)
    except ImportError:
        pass
    # Built-in fallback
    lat_lo, lat_hi = -90.0,  90.0
    lon_lo, lon_hi = -180.0, 180.0
    chars: List[str] = []
    bits = nibble = 0
    use_lon = True
    while len(chars) < precision:
        if use_lon:
            mid = (lon_lo + lon_hi) / 2
            if lon >= mid:
                nibble = (nibble << 1) | 1
                lon_lo = mid
            else:
                nibble <<= 1
                lon_hi = mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                nibble = (nibble << 1) | 1
                lat_lo = mid
            else:
                nibble <<= 1
                lat_hi = mid
        use_lon = not use_lon
        bits += 1
        if bits == 5:
            chars.append(_BASE32[nibble])
            nibble = bits = 0
    return "".join(chars)


# ---------------------------------------------------------------------------
# Great-circle math
# ---------------------------------------------------------------------------

def gc_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine great-circle distance in metres."""
    φ1, λ1 = math.radians(lat1), math.radians(lon1)
    φ2, λ2 = math.radians(lat2), math.radians(lon2)
    dφ = φ2 - φ1
    dλ = λ2 - λ1
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return 2 * EARTH_R * math.asin(math.sqrt(max(0.0, a)))


def gc_interpolate(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
    t: float,
) -> Tuple[float, float]:
    """Position at fraction t ∈ [0, 1] along the great-circle route."""
    φ1, λ1 = math.radians(lat1), math.radians(lon1)
    φ2, λ2 = math.radians(lat2), math.radians(lon2)
    d = gc_distance_m(lat1, lon1, lat2, lon2) / EARTH_R
    if d < 1e-9:
        return lat1, lon1
    sin_d = math.sin(d)
    A = math.sin((1 - t) * d) / sin_d
    B = math.sin(t * d) / sin_d
    x = A * math.cos(φ1) * math.cos(λ1) + B * math.cos(φ2) * math.cos(λ2)
    y = A * math.cos(φ1) * math.sin(λ1) + B * math.cos(φ2) * math.sin(λ2)
    z = A * math.sin(φ1) + B * math.sin(φ2)
    lat = math.degrees(math.atan2(z, math.sqrt(x ** 2 + y ** 2)))
    lon = math.degrees(math.atan2(y, x))
    return lat, lon


def initial_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing (degrees, 0 = N, clockwise) from point 1 to point 2."""
    φ1, λ1 = math.radians(lat1), math.radians(lon1)
    φ2, λ2 = math.radians(lat2), math.radians(lon2)
    dλ = λ2 - λ1
    x = math.sin(dλ) * math.cos(φ2)
    y = math.cos(φ1) * math.sin(φ2) - math.sin(φ1) * math.cos(φ2) * math.cos(dλ)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def point_at_bearing(
    lat: float, lon: float,
    bearing_deg: float,
    dist_m: float,
) -> Tuple[float, float]:
    """Destination point given start, bearing, and distance."""
    φ = math.radians(lat)
    λ = math.radians(lon)
    θ = math.radians(bearing_deg)
    δ = dist_m / EARTH_R
    φ2 = math.asin(
        math.sin(φ) * math.cos(δ) + math.cos(φ) * math.sin(δ) * math.cos(θ)
    )
    λ2 = λ + math.atan2(
        math.sin(θ) * math.sin(δ) * math.cos(φ),
        math.cos(δ) - math.sin(φ) * math.sin(φ2),
    )
    return math.degrees(φ2), math.degrees(λ2)


# ---------------------------------------------------------------------------
# Aircraft model
# ---------------------------------------------------------------------------

class FlightPhase(Enum):
    CLIMBING    = "CLIMBING"
    CRUISING    = "CRUISING"
    DESCENDING  = "DESCENDING"


@dataclass
class Aircraft:
    # Identity
    icao24:          str
    callsign:        str
    origin_country:  str
    squawk:          str

    # Route end-points
    orig_lat:   float
    orig_lon:   float
    orig_elev_m: float
    dest_lat:   float
    dest_lon:   float
    dest_elev_m: float
    route_dist_m: float

    # Performance envelope
    cruise_alt_m:   float   # assigned cruise altitude (metres)
    cruise_speed_ms: float  # true airspeed at cruise (m/s)

    # Dynamic state
    progress:         float        # 0.0 (origin) → 1.0 (destination)
    lat:              float
    lon:              float
    alt_m:            float
    track_deg:        float
    vertical_rate_ms: float
    phase:            FlightPhase

    # Noise characteristics (assigned once at creation, vary per aircraft)
    baro_geo_offset_m: float  # constant baro-minus-geo offset for this airframe
    ping_miss_prob:    float  # probability of skipping a position update
    next_update_t:     float  # wall-clock time of next ping

    # Near-miss tagging (None for regular aircraft)
    near_miss_pair: Optional[str] = None
    # When True, skip altitude transitions (used for near-miss ghost aircraft)
    flat_cruise: bool = False

    # ------------------------------------------------------------------ #

    def advance(self, dt: float) -> None:
        """Update position and altitude for a time step of dt seconds."""
        speed = self.cruise_speed_ms
        if not self.flat_cruise and self.phase != FlightPhase.CRUISING:
            speed *= 0.73  # slower during climb/descent

        self.progress = min(1.0, self.progress + speed * dt / self.route_dist_m)

        # Position via great-circle interpolation
        self.lat, self.lon = gc_interpolate(
            self.orig_lat, self.orig_lon,
            self.dest_lat, self.dest_lon,
            self.progress,
        )

        # Track: bearing toward destination from current position
        if self.progress < 0.999:
            self.track_deg = initial_bearing(
                self.lat, self.lon, self.dest_lat, self.dest_lon
            )

        # Altitude profile
        if self.flat_cruise:
            self.phase = FlightPhase.CRUISING
            self.alt_m = self.cruise_alt_m
            self.vertical_rate_ms = 0.0
            return

        dist_flown_m    = self.progress * self.route_dist_m
        dist_remaining_m = (1.0 - self.progress) * self.route_dist_m
        climb_dist_m    = self.route_dist_m * CLIMB_FRAC
        descent_dist_m  = self.route_dist_m * DESCENT_FRAC

        if dist_flown_m < climb_dist_m:
            self.phase = FlightPhase.CLIMBING
            t = dist_flown_m / climb_dist_m
            self.alt_m = self.orig_elev_m + t * (self.cruise_alt_m - self.orig_elev_m)
            self.vertical_rate_ms = CLIMB_RATE_MS
        elif dist_remaining_m < descent_dist_m:
            self.phase = FlightPhase.DESCENDING
            t = dist_remaining_m / descent_dist_m
            self.alt_m = self.dest_elev_m + t * (self.cruise_alt_m - self.dest_elev_m)
            self.vertical_rate_ms = -DESCENT_RATE_MS
        else:
            self.phase = FlightPhase.CRUISING
            self.alt_m = self.cruise_alt_m
            self.vertical_rate_ms = 0.0

    def to_message(self, now: float) -> dict:
        """Serialise to an ADS-B state-vector dict (OpenSky Network format)."""
        # GPS jitter  ±50 m ≈ ±0.00045°
        jitter = 0.00045
        lat = self.lat + random.gauss(0, jitter)
        lon = self.lon + random.gauss(0, jitter)

        baro_alt_m  = self.alt_m + random.gauss(0, 8)
        geo_alt_m   = self.alt_m + self.baro_geo_offset_m + random.gauss(0, 4)

        # Speed in m/s — slower during climb/descent
        base_speed = self.cruise_speed_ms
        if not self.flat_cruise and self.phase != FlightPhase.CRUISING:
            base_speed *= 0.73
        velocity_ms = base_speed + random.gauss(0, 1.5)

        vrate_ms  = self.vertical_rate_ms + random.gauss(0, 0.3)
        track_deg = (self.track_deg + random.gauss(0, 0.4)) % 360

        gh = geohash_encode(lat, lon, precision=4)

        return {
            "icao24":        self.icao24,
            "callsign":      self.callsign.strip(),
            "origin_country": self.origin_country,
            "time_position": round(now, 3),
            "last_contact":  round(now, 3),
            "longitude":     round(lon, 6),
            "latitude":      round(lat, 6),
            "baro_altitude": round(baro_alt_m, 1),
            "on_ground":     False,
            "velocity":      round(velocity_ms, 2),
            "true_track":    round(track_deg, 2),
            "vertical_rate": round(vrate_ms, 3),
            "geo_altitude":  round(geo_alt_m, 1),
            "squawk":        self.squawk,
            "geohash":       gh,
            "_sim":          True,
            "_near_miss_pair": self.near_miss_pair,
        }


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------

def _random_icao(seed: int) -> str:
    random.seed(seed + int(time.time() * 1000) % 99991)
    return f"{random.randint(0, 0xFFFFFF):06x}"


def _random_callsign() -> str:
    airline = random.choice(_AIRLINES)
    number  = random.randint(1, 9999)
    return f"{airline}{number:<4d}"


def _random_squawk() -> str:
    return f"{random.randint(0, 7):01d}{random.randint(0, 7):01d}" \
           f"{random.randint(0, 7):01d}{random.randint(0, 7):01d}"


def make_aircraft(counter: int, now: float) -> Aircraft:
    """Create a normal aircraft on a random US airport-to-airport route."""
    orig, dest = random.sample(AIRPORTS, 2)
    orig_icao, orig_lat, orig_lon, orig_elev_m = orig
    dest_icao, dest_lat, dest_lon, dest_elev_m = dest

    dist_m = gc_distance_m(orig_lat, orig_lon, dest_lat, dest_lon)

    cruise_alt_m   = random.uniform(MIN_CRUISE_ALT_M, MAX_CRUISE_ALT_M)
    cruise_speed_ms = random.uniform(380, 470) * KTS_TO_MS

    # Start at a random point along the route so all aircraft aren't bunched
    progress = random.uniform(0.0, 1.0)
    lat, lon = gc_interpolate(orig_lat, orig_lon, dest_lat, dest_lon, progress)

    # Approximate altitude at starting progress
    dist_flown_m    = progress * dist_m
    dist_remaining_m = (1.0 - progress) * dist_m
    climb_dist_m    = dist_m * CLIMB_FRAC
    descent_dist_m  = dist_m * DESCENT_FRAC
    if dist_flown_m < climb_dist_m:
        t     = dist_flown_m / climb_dist_m
        alt_m = orig_elev_m + t * (cruise_alt_m - orig_elev_m)
        phase = FlightPhase.CLIMBING
        vrate = CLIMB_RATE_MS
    elif dist_remaining_m < descent_dist_m:
        t     = dist_remaining_m / descent_dist_m
        alt_m = dest_elev_m + t * (cruise_alt_m - dest_elev_m)
        phase = FlightPhase.DESCENDING
        vrate = -DESCENT_RATE_MS
    else:
        alt_m = cruise_alt_m
        phase = FlightPhase.CRUISING
        vrate = 0.0

    track = initial_bearing(lat, lon, dest_lat, dest_lon)

    return Aircraft(
        icao24           = _random_icao(counter),
        callsign         = _random_callsign(),
        origin_country   = "United States",
        squawk           = _random_squawk(),
        orig_lat         = orig_lat,
        orig_lon         = orig_lon,
        orig_elev_m      = orig_elev_m,
        dest_lat         = dest_lat,
        dest_lon         = dest_lon,
        dest_elev_m      = dest_elev_m,
        route_dist_m     = dist_m,
        cruise_alt_m     = cruise_alt_m,
        cruise_speed_ms  = cruise_speed_ms,
        progress         = progress,
        lat              = lat,
        lon              = lon,
        alt_m            = alt_m,
        track_deg        = track,
        vertical_rate_ms = vrate,
        phase            = phase,
        baro_geo_offset_m = random.uniform(-120, 120),
        ping_miss_prob   = random.uniform(0.01, 0.08),
        next_update_t    = now + random.uniform(0, 2.0),
    )


def reassign_route(ac: Aircraft, now: float) -> Aircraft:
    """Give a landed aircraft a new route departing from its destination."""
    orig_icao, orig_lat, orig_lon, orig_elev_m = (
        "ZZZZ", ac.dest_lat, ac.dest_lon, ac.dest_elev_m
    )
    # pick a new destination different from current
    dest = random.choice([a for a in AIRPORTS
                          if abs(a[1] - ac.dest_lat) > 0.1 or abs(a[2] - ac.dest_lon) > 0.1])
    dest_icao, dest_lat, dest_lon, dest_elev_m = dest

    dist_m = gc_distance_m(orig_lat, orig_lon, dest_lat, dest_lon)
    lat, lon = orig_lat, orig_lon
    track = initial_bearing(lat, lon, dest_lat, dest_lon)

    ac.orig_lat       = orig_lat
    ac.orig_lon       = orig_lon
    ac.orig_elev_m    = orig_elev_m
    ac.dest_lat       = dest_lat
    ac.dest_lon       = dest_lon
    ac.dest_elev_m    = dest_elev_m
    ac.route_dist_m   = dist_m
    ac.cruise_alt_m   = random.uniform(MIN_CRUISE_ALT_M, MAX_CRUISE_ALT_M)
    ac.cruise_speed_ms = random.uniform(380, 470) * KTS_TO_MS
    ac.progress       = 0.0
    ac.lat            = lat
    ac.lon            = lon
    ac.alt_m          = orig_elev_m
    ac.track_deg      = track
    ac.vertical_rate_ms = 0.0
    ac.phase          = FlightPhase.CLIMBING
    ac.next_update_t  = now + random.uniform(0.5, 2.0)
    ac.near_miss_pair = None
    ac.flat_cruise    = False
    return ac


def make_near_miss_pair(counter: int, now: float) -> Tuple["Aircraft", "Aircraft"]:
    """
    Create two aircraft that will converge to a near-miss event.

    Geometry:
      - Random convergence point in US airspace
      - Aircraft A approaches from bearing B, starts NM_APPROACH_M away
      - Aircraft B approaches from bearing B+170°±20°, starts NM_APPROACH_M away
      - Both destined for a point NM_OVERSHOOT_M past the convergence point
        so they fly *through* each other (crossing scenario)
      - Both cruise at the same altitude ± 200 ft (< 1 000 ft vertical sep → LoS)
    """
    # Convergence point
    c_lat = random.uniform(*_US_LAT)
    c_lon = random.uniform(*_US_LON)
    c_alt_m = random.uniform(MIN_CRUISE_ALT_M, MAX_CRUISE_ALT_M)

    # Two approach bearings ~opposite (170°–190° apart)
    b1 = random.uniform(0, 360)
    b2 = (b1 + 170 + random.uniform(0, 20)) % 360

    # Start positions (NM_APPROACH_M behind convergence)
    a1_lat, a1_lon = point_at_bearing(c_lat, c_lon, (b1 + 180) % 360, NM_APPROACH_M)
    a2_lat, a2_lon = point_at_bearing(c_lat, c_lon, (b2 + 180) % 360, NM_APPROACH_M)

    # Destinations (NM_OVERSHOOT_M past convergence)
    d1_lat, d1_lon = point_at_bearing(c_lat, c_lon, b1, NM_OVERSHOOT_M)
    d2_lat, d2_lon = point_at_bearing(c_lat, c_lon, b2, NM_OVERSHOOT_M)

    route1_m = gc_distance_m(a1_lat, a1_lon, d1_lat, d1_lon)
    route2_m = gc_distance_m(a2_lat, a2_lon, d2_lat, d2_lon)

    speed_ms = random.uniform(410, 460) * KTS_TO_MS

    # Vertical separation at convergence < 800 ft → Loss of Separation
    alt_offset_m = random.uniform(0, 240)  # 0–787 ft offset between the two

    icao_a = f"nm{counter:04x}"
    icao_b = f"nm{counter + 1:04x}"

    def _make(icao, partner_icao, s_lat, s_lon, d_lat, d_lon, route_m, alt_m):
        return Aircraft(
            icao24           = icao,
            callsign         = _random_callsign(),
            origin_country   = "United States",
            squawk           = _random_squawk(),
            orig_lat         = s_lat,
            orig_lon         = s_lon,
            orig_elev_m      = alt_m,       # unused (flat_cruise=True)
            dest_lat         = d_lat,
            dest_lon         = d_lon,
            dest_elev_m      = alt_m,       # unused
            route_dist_m     = route_m,
            cruise_alt_m     = alt_m,
            cruise_speed_ms  = speed_ms,
            progress         = 0.0,
            lat              = s_lat,
            lon              = s_lon,
            alt_m            = alt_m,
            track_deg        = initial_bearing(s_lat, s_lon, d_lat, d_lon),
            vertical_rate_ms = 0.0,
            phase            = FlightPhase.CRUISING,
            baro_geo_offset_m = random.uniform(-80, 80),
            ping_miss_prob   = 0.02,
            next_update_t    = now,
            flat_cruise      = True,
            near_miss_pair   = partner_icao,
        )

    ac_a = _make(icao_a, icao_b, a1_lat, a1_lon, d1_lat, d1_lon, route1_m, c_alt_m)
    ac_b = _make(icao_b, icao_a, a2_lat, a2_lon, d2_lat, d2_lon, route2_m,
                 c_alt_m + alt_offset_m)

    return ac_a, ac_b


# ---------------------------------------------------------------------------
# Kafka producer wrapper  (supports confluent-kafka, kafka-python, dry-run)
# ---------------------------------------------------------------------------

class _ProducerWrapper:
    """Thin wrapper so the simulator is not coupled to a specific Kafka client."""

    def __init__(self, bootstrap_servers: str, dry_run: bool = False):
        self._dry_run = dry_run
        self._backend: Optional[str] = None
        self._producer = None
        if not dry_run:
            self._connect(bootstrap_servers)

    def _connect(self, bootstrap_servers: str) -> None:
        # Try confluent-kafka first (recommended for high throughput)
        try:
            from confluent_kafka import Producer  # type: ignore
            self._producer = Producer({"bootstrap.servers": bootstrap_servers})
            self._backend  = "confluent"
            log.info("Kafka backend: confluent-kafka  bootstrap=%s", bootstrap_servers)
            return
        except ImportError:
            pass

        # Fall back to kafka-python
        try:
            from kafka import KafkaProducer  # type: ignore
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                linger_ms=50,
                compression_type="gzip",
            )
            self._backend = "kafka-python"
            log.info("Kafka backend: kafka-python  bootstrap=%s", bootstrap_servers)
            return
        except ImportError:
            pass

        log.warning(
            "Neither confluent-kafka nor kafka-python found — "
            "messages will be written to stdout only. "
            "Install one of them:  pip install confluent-kafka  OR  pip install kafka-python"
        )
        self._backend = "stdout"

    def produce(self, topic: str, key: str, value: dict) -> None:
        if self._dry_run or self._backend == "stdout":
            log.debug("DRY-RUN  key=%-8s  %s", key, json.dumps(value)[:120])
            return
        encoded_key   = key.encode()
        encoded_value = json.dumps(value).encode()
        if self._backend == "confluent":
            self._producer.produce(topic, key=encoded_key, value=encoded_value)
            self._producer.poll(0)
        elif self._backend == "kafka-python":
            self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        if self._dry_run or self._backend in (None, "stdout"):
            return
        if self._backend == "confluent":
            self._producer.flush(timeout=10)
        elif self._backend == "kafka-python":
            self._producer.flush(timeout=10)


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class ADSBSimulator:
    """
    Main simulator loop.

    Maintains a fleet of N aircraft plus dynamically injected near-miss pairs.
    Produces state vectors to Kafka at a realistic rate (1–2 s per aircraft).
    """

    def __init__(
        self,
        n_aircraft:     int   = 500,
        near_miss_rate: float = 0.01,
        producer:       Optional[_ProducerWrapper] = None,
        log_interval:   int   = 10,
    ):
        self.n_aircraft     = n_aircraft
        self.near_miss_rate = near_miss_rate
        self.producer       = producer or _ProducerWrapper("", dry_run=True)
        self.log_interval   = log_interval

        self._running = True
        self._icao_counter = 0    # monotonic counter for unique ICAOs

        # Metrics
        self._msgs_sent   = 0
        self._nm_events   = 0
        self._last_log_t  = time.time()
        self._last_nm_t   = time.time()
        self._window_msgs = 0

        # Install SIGINT / SIGTERM handlers for graceful shutdown
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log.info(
            "Initialising simulator: %d aircraft, near-miss-rate=%.3f",
            n_aircraft, near_miss_rate,
        )
        now = time.time()
        self._aircraft: List[Aircraft] = [
            make_aircraft(i, now) for i in range(n_aircraft)
        ]
        self._icao_counter = n_aircraft

    def _handle_signal(self, signum, frame) -> None:
        log.info("Shutdown signal received — flushing and exiting…")
        self._running = False

    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """Main simulation loop. Blocks until SIGINT / SIGTERM."""
        last_tick = time.time()
        log.info("Simulator running.  Press Ctrl-C to stop.")

        while self._running:
            now = time.time()
            dt  = now - last_tick
            last_tick = now

            # ── Advance and emit aircraft ──────────────────────────────
            to_reassign: List[int] = []
            for idx, ac in enumerate(self._aircraft):
                if now < ac.next_update_t:
                    continue

                # Occasional missed ping
                if random.random() < ac.ping_miss_prob:
                    ac.next_update_t = now + random.uniform(1.0, 2.5)
                    continue

                ac.advance(dt)
                msg = ac.to_message(now)
                self.producer.produce(TOPIC_RAW, key=msg["geohash"] or "0000", value=msg)
                self._msgs_sent  += 1
                self._window_msgs += 1
                ac.next_update_t  = now + random.uniform(1.0, 2.0)

                if ac.progress >= 1.0:
                    if ac.flat_cruise:
                        # Near-miss aircraft finished — remove
                        to_reassign.append(idx)
                    else:
                        reassign_route(ac, now)

            # Remove completed near-miss aircraft (iterate in reverse to keep indices valid)
            for idx in reversed(to_reassign):
                self._aircraft.pop(idx)

            # ── Near-miss injection (every 10-second window) ───────────
            if now - self._last_nm_t >= 10.0:
                self._last_nm_t = now
                if random.random() < self.near_miss_rate:
                    ac_a, ac_b = make_near_miss_pair(self._icao_counter, now)
                    self._icao_counter += 2
                    self._aircraft.extend([ac_a, ac_b])
                    self._nm_events += 1
                    log.info(
                        "⚠  Near-miss scenario injected  "
                        "pair=(%s, %s)  total_injected=%d",
                        ac_a.icao24, ac_b.icao24, self._nm_events,
                    )

            # ── Stats log ─────────────────────────────────────────────
            elapsed = now - self._last_log_t
            if elapsed >= self.log_interval:
                rate = self._window_msgs / elapsed
                n_nm = sum(1 for a in self._aircraft if a.near_miss_pair)
                log.info(
                    "Stats │ rate=%5.0f msg/s │ total=%d │ "
                    "fleet=%d (incl. %d near-miss) │ scenarios=%d",
                    rate, self._msgs_sent,
                    len(self._aircraft), n_nm, self._nm_events,
                )
                self._last_log_t  = now
                self._window_msgs = 0

            # ── Throttle loop to ~20 Hz ────────────────────────────────
            loop_elapsed = time.time() - now
            sleep_s = max(0, 0.05 - loop_elapsed)
            if sleep_s:
                time.sleep(sleep_s)

        # Graceful shutdown
        log.info("Flushing Kafka producer…")
        self.producer.flush()
        log.info(
            "Simulator stopped. Total messages sent: %d  Near-miss scenarios: %d",
            self._msgs_sent, self._nm_events,
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ADS-B Simulator — produce synthetic aircraft state vectors to Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--aircraft", "-n",
        type=int, default=500,
        metavar="N",
        help="Number of aircraft in the simulation fleet",
    )
    parser.add_argument(
        "--near-miss-rate", "-r",
        type=float, default=0.01,
        metavar="RATE",
        help="Probability [0–1] that a 10-second window contains a near-miss injection",
    )
    parser.add_argument(
        "--bootstrap", "-b",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        metavar="HOST:PORT",
        help="Kafka bootstrap servers (comma-separated)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Kafka; log messages to stdout (useful without a running Kafka)",
    )
    parser.add_argument(
        "--log-interval",
        type=int, default=10,
        metavar="SECS",
        help="How often to print throughput stats (seconds)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging (prints every message in dry-run mode)",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    producer = _ProducerWrapper(args.bootstrap, dry_run=args.dry_run)

    sim = ADSBSimulator(
        n_aircraft     = args.aircraft,
        near_miss_rate = args.near_miss_rate,
        producer       = producer,
        log_interval   = args.log_interval,
    )
    sim.run()


if __name__ == "__main__":
    main()
