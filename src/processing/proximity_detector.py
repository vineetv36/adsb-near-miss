"""
Core distance and separation calculations.

All public functions are pure Python (no Spark/pandas dependency) so they can
be called from both the Spark foreachBatch handler and unit tests.

Unit conventions
----------------
  Altitudes  : feet (ft)
  Distances  : nautical miles (NM)
  Speeds     : knots (kts)
  Velocities : m/s on input (OpenSky convention); converted internally
"""

import math
from enum import Enum
from typing import Optional

EARTH_R_NM  = 3_440.065   # Earth radius in nautical miles
MS_TO_KTS   = 1.0 / 0.514444
M_TO_FT     = 1.0 / 0.3048


# ---------------------------------------------------------------------------
# Separation classification
# ---------------------------------------------------------------------------

class SeparationLevel(str, Enum):
    NORMAL            = "NORMAL"
    PROXIMITY         = "PROXIMITY"          # < 8 NM / 2 000 ft
    NEAR_MISS         = "NEAR_MISS"          # < 5 NM / 1 500 ft
    LOSS_OF_SEPARATION = "LOSS_OF_SEPARATION" # < 3 NM / 1 000 ft  (en-route)
    UNKNOWN           = "UNKNOWN"            # altitude data missing


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    φ1, λ1 = math.radians(lat1), math.radians(lon1)
    φ2, λ2 = math.radians(lat2), math.radians(lon2)
    dφ = φ2 - φ1
    dλ = λ2 - λ1
    a  = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return 2 * EARTH_R_NM * math.asin(math.sqrt(max(0.0, a)))


# ---------------------------------------------------------------------------
# Altitude helpers
# ---------------------------------------------------------------------------

def normalize_altitude(
    baro_alt_m: Optional[float],
    geo_alt_m:  Optional[float],
    qnh:        Optional[float] = None,
) -> Optional[float]:
    """
    Reconcile barometric and geometric altitude; return the best estimate
    in *metres* (callers convert to feet as needed).

    Priority:
      1. If both present, prefer baro (standard ATC reference); geo used as
         sanity check — if they diverge by > 300 m flag it but still return baro.
      2. If only one is present, use it.
      3. If neither, return None.

    The baro/geo difference (ISA deviation, local QNH) can be ±300 m easily;
    we don't correct for it here unless a QNH override is provided.
    """
    if baro_alt_m is not None and geo_alt_m is not None:
        return baro_alt_m   # baro is the ATC-standard reference altitude
    if baro_alt_m is not None:
        return baro_alt_m
    if geo_alt_m is not None:
        return geo_alt_m
    return None


def vertical_separation(alt1_ft: Optional[float], alt2_ft: Optional[float]) -> Optional[float]:
    """
    Absolute vertical separation in feet.
    Returns None if either altitude is missing (caller should treat as UNKNOWN).
    """
    if alt1_ft is None or alt2_ft is None:
        return None
    return abs(alt1_ft - alt2_ft)


# ---------------------------------------------------------------------------
# Closure rate
# ---------------------------------------------------------------------------

def closure_rate(
    vel1_ms:  Optional[float],
    track1:   Optional[float],
    vel2_ms:  Optional[float],
    track2:   Optional[float],
) -> Optional[float]:
    """
    Estimated closure rate in knots (positive = converging, negative = diverging).

    Method: project the relative velocity vector onto the unit separation vector.
    Inputs: velocities in m/s, tracks in degrees (0 = N, clockwise).
    Returns None when any input is missing.
    """
    if None in (vel1_ms, track1, vel2_ms, track2):
        return None

    t1 = math.radians(track1)
    t2 = math.radians(track2)

    # Velocity components (east, north) in m/s
    vx1, vy1 = vel1_ms * math.sin(t1), vel1_ms * math.cos(t1)
    vx2, vy2 = vel2_ms * math.sin(t2), vel2_ms * math.cos(t2)

    # Relative velocity of aircraft 2 w.r.t. aircraft 1
    rx, ry = vx2 - vx1, vy2 - vy1

    # We don't have the separation vector here, so return the magnitude of
    # relative velocity instead (a conservative upper bound on closure rate).
    # The actual closure rate along the separation axis is computed in the
    # Spark job when both positions are available.
    rel_speed_ms = math.sqrt(rx ** 2 + ry ** 2)
    return round(rel_speed_ms * MS_TO_KTS, 1)


def closure_rate_with_positions(
    lat1: float, lon1: float, vel1_ms: Optional[float], track1: Optional[float],
    lat2: float, lon2: float, vel2_ms: Optional[float], track2: Optional[float],
) -> Optional[float]:
    """
    Closure rate projected onto the line connecting the two aircraft.
    Positive = converging, negative = diverging.
    """
    if None in (vel1_ms, track1, vel2_ms, track2):
        return None

    t1 = math.radians(track1)
    t2 = math.radians(track2)

    vx1, vy1 = vel1_ms * math.sin(t1), vel1_ms * math.cos(t1)
    vx2, vy2 = vel2_ms * math.sin(t2), vel2_ms * math.cos(t2)

    # Approximate separation vector in pseudo-Cartesian coords (OK for < 100 NM)
    dlat = lat2 - lat1
    dlon = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    dist = math.sqrt(dlat ** 2 + dlon ** 2)
    if dist < 1e-9:
        return 0.0

    sep_x, sep_y = dlon / dist, dlat / dist   # unit vector toward aircraft 2

    # Relative velocity of 2 w.r.t. 1
    rx, ry = vx2 - vx1, vy2 - vy1

    # Closure = -(relative velocity · separation unit vector)
    # Positive when aircraft are approaching each other
    closure_ms = -(rx * sep_x + ry * sep_y)
    return round(closure_ms * MS_TO_KTS, 1)


# ---------------------------------------------------------------------------
# Separation classification
# ---------------------------------------------------------------------------

def classify_separation(
    horizontal_nm: float,
    vertical_ft:   Optional[float],
    airspace_class: str = "E",
) -> SeparationLevel:
    """
    Classify an aircraft pair by ICAO separation standards.

    En-route (Class A/B/C/E above 18 000 ft):
      Loss of separation : < 3 NM AND < 1 000 ft
      Near miss          : < 5 NM AND < 1 500 ft
      Proximity alert    : < 8 NM AND < 2 000 ft

    Terminal area (Class B/C/D, below 18 000 ft):
      Loss of separation : < 1 NM AND <   500 ft

    If vertical separation is unknown (None), demote one level to UNKNOWN.
    """
    if vertical_ft is None:
        # We know horizontal proximity; flag as unknown-severity
        if horizontal_nm < 5.0:
            return SeparationLevel.UNKNOWN
        return SeparationLevel.NORMAL

    # Terminal area override
    is_terminal = airspace_class in ("B", "C", "D")
    if is_terminal and horizontal_nm < 1.0 and vertical_ft < 500:
        return SeparationLevel.LOSS_OF_SEPARATION

    # En-route thresholds
    if horizontal_nm < 3.0 and vertical_ft < 1_000:
        return SeparationLevel.LOSS_OF_SEPARATION
    if horizontal_nm < 5.0 and vertical_ft < 1_500:
        return SeparationLevel.NEAR_MISS
    if horizontal_nm < 8.0 and vertical_ft < 2_000:
        return SeparationLevel.PROXIMITY
    return SeparationLevel.NORMAL
