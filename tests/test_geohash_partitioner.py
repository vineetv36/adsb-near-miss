"""
Tests for src/processing/geohash_partitioner.py

Covers the pure-Python fallback implementation (which is always used in Spark
workers that lack the C extension) as well as the public API: encode,
decode_bounds, neighbors, and expand.
"""

import math

import pytest

from src.processing.geohash_partitioner import BBox, decode_bounds, encode, expand, neighbors

_BASE32_CHARS = set("0123456789bcdefghjkmnpqrstuvwxyz")


# ---------------------------------------------------------------------------
# encode
# ---------------------------------------------------------------------------


class TestEncode:
    def test_returns_string_of_correct_length(self):
        for precision in (1, 2, 3, 4, 5, 6):
            gh = encode(39.5, -98.35, precision=precision)
            assert isinstance(gh, str)
            assert len(gh) == precision

    def test_only_base32_characters(self):
        gh = encode(39.5, -98.35, precision=6)
        assert all(c in _BASE32_CHARS for c in gh), f"unexpected chars in {gh!r}"

    def test_deterministic(self):
        lat, lon = 40.6413, -73.7781
        assert encode(lat, lon, 5) == encode(lat, lon, 5)

    def test_precision4_is_prefix_of_precision5(self):
        lat, lon = 40.6413, -73.7781
        gh4 = encode(lat, lon, 4)
        gh5 = encode(lat, lon, 5)
        assert gh5.startswith(gh4), f"{gh5!r} does not start with {gh4!r}"

    def test_different_points_differ_at_precision5(self):
        # LAX vs JFK — clear opposite coasts
        lax = encode(33.9425, -118.4081, 5)
        jfk = encode(40.6413, -73.7781, 5)
        assert lax != jfk

    def test_extreme_latitude_north(self):
        # Should not raise or produce invalid characters
        gh = encode(89.9, 0.0, 5)
        assert len(gh) == 5
        assert all(c in _BASE32_CHARS for c in gh)

    def test_extreme_latitude_south(self):
        gh = encode(-89.9, 0.0, 5)
        assert len(gh) == 5

    def test_antimeridian_east(self):
        gh = encode(35.0, 179.9, 5)
        assert len(gh) == 5

    def test_antimeridian_west(self):
        gh = encode(35.0, -179.9, 5)
        assert len(gh) == 5

    def test_zero_zero(self):
        # Gulf of Guinea — valid
        gh = encode(0.0, 0.0, 5)
        assert len(gh) == 5


# ---------------------------------------------------------------------------
# decode_bounds
# ---------------------------------------------------------------------------


class TestDecodeBounds:
    def test_returns_bbox(self):
        bb = decode_bounds(encode(39.5, -98.35, 5))
        assert isinstance(bb, BBox)

    def test_input_coordinates_inside_bounds(self):
        lat, lon = 40.6413, -73.7781
        gh = encode(lat, lon, 5)
        bb = decode_bounds(gh)
        assert bb.lat_min <= lat <= bb.lat_max
        assert bb.lon_min <= lon <= bb.lon_max

    def test_center_properties(self):
        gh = encode(39.5, -98.35, 5)
        bb = decode_bounds(gh)
        assert bb.lat_center == pytest.approx((bb.lat_min + bb.lat_max) / 2)
        assert bb.lon_center == pytest.approx((bb.lon_min + bb.lon_max) / 2)

    def test_span_properties(self):
        bb = decode_bounds(encode(39.5, -98.35, 5))
        assert bb.lat_span == pytest.approx(bb.lat_max - bb.lat_min)
        assert bb.lon_span == pytest.approx(bb.lon_max - bb.lon_min)

    def test_precision4_cell_roughly_20km(self):
        """Precision-4 cells are approximately 39 km × 20 km."""
        gh4 = encode(39.5, -98.35, 4)
        bb = decode_bounds(gh4)
        lat_span_km = bb.lat_span * 111.0
        lon_span_km = bb.lon_span * 111.0 * math.cos(math.radians(bb.lat_center))
        # Allow a wide tolerance; pure-Python and C-extension may disagree slightly
        assert 15 < lat_span_km < 30, f"lat span {lat_span_km:.1f} km unexpected"
        assert 30 < lon_span_km < 55, f"lon span {lon_span_km:.1f} km unexpected"

    def test_precision5_smaller_than_precision4(self):
        lat, lon = 39.5, -98.35
        bb4 = decode_bounds(encode(lat, lon, 4))
        bb5 = decode_bounds(encode(lat, lon, 5))
        assert bb5.lat_span < bb4.lat_span
        assert bb5.lon_span < bb4.lon_span

    def test_higher_precision_tighter_bounds(self):
        lat, lon = 33.9425, -118.4081
        prev_area = float("inf")
        for p in (1, 2, 3, 4, 5, 6):
            bb = decode_bounds(encode(lat, lon, p))
            area = bb.lat_span * bb.lon_span
            assert area < prev_area
            prev_area = area


# ---------------------------------------------------------------------------
# neighbors
# ---------------------------------------------------------------------------


class TestNeighbors:
    def test_returns_eight_cells(self):
        assert len(neighbors("dr5ru")) == 8

    def test_all_same_precision(self):
        for n in neighbors("dr5ru"):
            assert len(n) == 5, f"neighbor {n!r} has wrong length"

    def test_all_valid_base32(self):
        for n in neighbors("dr5ru"):
            assert all(c in _BASE32_CHARS for c in n)

    def test_all_distinct(self):
        nb = neighbors("dr5ru")
        assert len(set(nb)) == 8, "all 8 neighbors must be distinct"

    def test_self_not_in_neighbors(self):
        gh = "dr5ru"
        assert gh not in neighbors(gh)

    def test_neighbors_geographically_adjacent(self):
        """Each neighbor's center should be roughly one cell width away."""
        gh = "dr5ru"
        bb = decode_bounds(gh)
        for n in neighbors(gh):
            nbb = decode_bounds(n)
            dlat = abs(nbb.lat_center - bb.lat_center)
            dlon = abs(nbb.lon_center - bb.lon_center)
            # One step = 1 cell width; allow up to 2.5 cell widths for diagonals
            assert dlat <= bb.lat_span * 2.5 or dlon <= bb.lon_span * 2.5, (
                f"neighbor {n!r} is too far from {gh!r}"
            )

    def test_precision4_neighbors(self):
        assert len(neighbors("dr5r")) == 8

    def test_mutual_neighborhood(self):
        """If B is a neighbor of A, A must be a neighbor of B."""
        gh = "9q8yy"  # San Francisco area
        for n in neighbors(gh):
            assert gh in neighbors(n), f"{gh!r} not in neighbors of its own neighbor {n!r}"


# ---------------------------------------------------------------------------
# expand
# ---------------------------------------------------------------------------


class TestExpand:
    def test_returns_nine_cells(self):
        assert len(expand("dr5ru")) == 9

    def test_includes_self(self):
        gh = "dr5ru"
        assert gh in expand(gh)

    def test_all_distinct(self):
        cells = expand("dr5ru")
        assert len(set(cells)) == 9, "all 9 cells in expand() must be distinct"

    def test_is_self_union_neighbors(self):
        gh = "dr5ru"
        assert set(expand(gh)) == {gh} | set(neighbors(gh))

    def test_expand_precision4(self):
        cells = expand(encode(39.5, -98.35, 4))
        assert len(cells) == 9
        assert all(len(c) == 4 for c in cells)

    def test_all_cells_valid_base32(self):
        for c in expand("9q8yy"):
            assert all(ch in _BASE32_CHARS for ch in c)

    @pytest.mark.parametrize("lat,lon", [
        (40.6413, -73.7781),   # JFK
        (33.9425, -118.4081),  # LAX
        (41.9742, -87.9073),   # ORD
        (25.7959, -80.2870),   # MIA
    ])
    def test_expand_us_airports(self, lat, lon):
        gh = encode(lat, lon, 4)
        cells = expand(gh)
        assert len(cells) == 9
        assert gh in cells
