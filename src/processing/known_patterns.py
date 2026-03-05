"""
Known-pattern tagger for proximity events.

Tags events that match expected *safe* flight patterns so downstream hotspot
analysis can exclude or segment them.  Events are NEVER discarded — they keep
their separation severity; only the `pattern` field is populated.

Patterns recognised
-------------------
  FORMATION_FLIGHT  — Very close pair with near-zero closure rate (military/aerobatic)
  HELICOPTER_OPS    — One aircraft below 500 ft AGL at low speed (< 100 kts)
  PARALLEL_ILS      — Both descending, similar headings, within closure of an airport
  DEPARTURE_CORRIDOR— Both climbing, similar headings
  HOLDING_PATTERN   — Track change rate suggests racetrack orbit
"""

import math
from typing import Optional

MS_TO_KTS = 1.0 / 0.514444

# Heading difference within which we consider two aircraft to share a track axis
_TRACK_TOLERANCE_DEG = 15.0


def _track_diff(t1: Optional[float], t2: Optional[float]) -> float:
    """Absolute angular difference between two headings, [0, 180]."""
    if t1 is None or t2 is None:
        return 180.0
    d = abs(t1 - t2) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _kts(vel_ms: Optional[float]) -> float:
    """Convert m/s to knots; return 0 if None."""
    if vel_ms is None:
        return 0.0
    return vel_ms * MS_TO_KTS


def tag_pattern(row: dict) -> Optional[str]:
    """
    Examine a proximity-event row and return the matching pattern name, or None.

    Expected keys (all Optional):
      icao24_a/b, lat_a/b, lon_a/b, alt_a_ft, alt_b_ft,
      velocity_a/b (m/s), true_track_a/b, vertical_rate_a/b (m/s),
      horiz_nm, vert_ft, clos_kt
    """
    horiz_nm   = row.get("horiz_nm", 999)
    vert_ft    = row.get("vert_ft",  999)
    clos_kt    = row.get("clos_kt")
    alt_a      = row.get("alt_a_ft")
    alt_b      = row.get("alt_b_ft")
    vel_a      = _kts(row.get("velocity_a"))
    vel_b      = _kts(row.get("velocity_b"))
    track_a    = row.get("true_track_a")
    track_b    = row.get("true_track_b")
    vrate_a_ms = row.get("vertical_rate_a", 0) or 0   # m/s
    vrate_b_ms = row.get("vertical_rate_b", 0) or 0

    # ── Formation flight ────────────────────────────────────────────────────
    # Very close (<0.5 NM) with near-zero closure rate → intentional formation
    if horiz_nm < 0.5 and (clos_kt is None or abs(clos_kt) < 15):
        return "FORMATION_FLIGHT"

    # ── Helicopter / low-altitude operations ────────────────────────────────
    # Either aircraft below 500 ft at < 100 kts
    low_a = (alt_a is not None and alt_a < 500)
    low_b = (alt_b is not None and alt_b < 500)
    slow_a = vel_a < 100
    slow_b = vel_b < 100
    if (low_a and slow_a) or (low_b and slow_b):
        return "HELICOPTER_OPS"

    # ── Parallel ILS approach ───────────────────────────────────────────────
    # Both descending (< -1 m/s), similar tracks (within ±15°)
    both_descending = (vrate_a_ms < -1.0 and vrate_b_ms < -1.0)
    similar_track   = _track_diff(track_a, track_b) < _TRACK_TOLERANCE_DEG
    if both_descending and similar_track:
        return "PARALLEL_ILS"

    # ── Departure corridor ──────────────────────────────────────────────────
    # Both climbing (> +1 m/s), similar tracks — same airport departure stream
    both_climbing = (vrate_a_ms > 1.0 and vrate_b_ms > 1.0)
    if both_climbing and similar_track:
        return "DEPARTURE_CORRIDOR"

    return None
