"""
Geohash encoding, decoding, and neighbor-cell logic.

Wraps python-geohash when available; falls back to a pure-Python implementation
so the module works in Spark workers that may not have the C extension installed.

Key design decision (from SPEC):
  Precision 5 → ~4.9 km × 4.9 km cells.  The widest separation threshold is
  8 NM (~15 km), so checking a cell + its 8 neighbors covers ~15 km in every
  direction — zero false negatives at cell borders, minimal false-positive
  candidate pairs.
"""

from typing import List, NamedTuple, Tuple

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_DECODE = {c: i for i, c in enumerate(_BASE32)}


# ---------------------------------------------------------------------------
# Encode
# ---------------------------------------------------------------------------

def encode(lat: float, lon: float, precision: int = 5) -> str:
    """Return the geohash string for (lat, lon) at the given precision."""
    try:
        import geohash as _gh
        return _gh.encode(lat, lon, precision)
    except ImportError:
        pass
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
# Decode bounds
# ---------------------------------------------------------------------------

class BBox(NamedTuple):
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    @property
    def lat_center(self) -> float:
        return (self.lat_min + self.lat_max) / 2

    @property
    def lon_center(self) -> float:
        return (self.lon_min + self.lon_max) / 2

    @property
    def lat_span(self) -> float:
        return self.lat_max - self.lat_min

    @property
    def lon_span(self) -> float:
        return self.lon_max - self.lon_min


def decode_bounds(gh: str) -> BBox:
    """Return the bounding box for a geohash cell."""
    try:
        import geohash as _gh
        lat, lon, lat_err, lon_err = _gh.decode_exactly(gh)
        return BBox(lat - lat_err, lat + lat_err, lon - lon_err, lon + lon_err)
    except ImportError:
        pass
    lat_lo, lat_hi = -90.0,  90.0
    lon_lo, lon_hi = -180.0, 180.0
    use_lon = True
    for char in gh:
        d = _DECODE[char]
        for bit in range(4, -1, -1):
            if use_lon:
                mid = (lon_lo + lon_hi) / 2
                if d & (1 << bit):
                    lon_lo = mid
                else:
                    lon_hi = mid
            else:
                mid = (lat_lo + lat_hi) / 2
                if d & (1 << bit):
                    lat_lo = mid
                else:
                    lat_hi = mid
            use_lon = not use_lon
    return BBox(lat_lo, lat_hi, lon_lo, lon_hi)


# ---------------------------------------------------------------------------
# Neighbours
# ---------------------------------------------------------------------------

def neighbors(gh: str) -> List[str]:
    """Return the 8 geohash cells that surround *gh* (same precision)."""
    try:
        import geohash as _gh
        # python-geohash exposes neighbors() in some versions
        if hasattr(_gh, "neighbors"):
            return list(_gh.neighbors(gh).values())
    except ImportError:
        pass

    p   = len(gh)
    bb  = decode_bounds(gh)
    lat = bb.lat_center
    lon = bb.lon_center
    dlat = bb.lat_span
    dlon = bb.lon_span

    result: List[str] = []
    for row in (-1, 0, 1):
        for col in (-1, 0, 1):
            if row == 0 and col == 0:
                continue
            nlat = lat + row * dlat
            nlon = lon + col * dlon
            # Clamp / wrap
            nlat = max(-90.0 + 1e-9, min(90.0 - 1e-9, nlat))
            nlon = ((nlon + 180.0) % 360.0) - 180.0
            result.append(encode(nlat, nlon, p))
    return result


def expand(gh: str) -> List[str]:
    """Return *gh* plus its 8 neighbours (9 cells total)."""
    return [gh] + neighbors(gh)
