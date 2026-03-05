"""
Tests for src/processing/proximity_detector.py

All functions under test are pure Python with no external dependencies, so
the suite runs without Kafka, Spark, or any database connection.
"""

import math

import pytest

from src.processing.proximity_detector import (
    EARTH_R_NM,
    M_TO_FT,
    SeparationLevel,
    classify_separation,
    closure_rate,
    closure_rate_with_positions,
    haversine,
    normalize_altitude,
    vertical_separation,
)


# ---------------------------------------------------------------------------
# haversine
# ---------------------------------------------------------------------------


class TestHaversine:
    def test_same_point_is_zero(self):
        assert haversine(40.0, -74.0, 40.0, -74.0) == pytest.approx(0.0, abs=1e-9)

    def test_one_degree_latitude_approx_60nm(self):
        """1° of latitude ≈ 60.04 NM (Earth circumference / 360 × NM/km)."""
        d = haversine(0.0, 0.0, 1.0, 0.0)
        assert d == pytest.approx(60.04, rel=0.005)

    def test_one_degree_longitude_at_equator(self):
        """1° of longitude at the equator equals 1° of latitude in NM."""
        d = haversine(0.0, 0.0, 0.0, 1.0)
        assert d == pytest.approx(60.04, rel=0.005)

    def test_jfk_to_lhr(self):
        """JFK → LHR trans-Atlantic route ≈ 3,450–3,470 NM."""
        d = haversine(40.641, -73.778, 51.477, -0.461)
        assert 3_440 < d < 3_500

    def test_lax_to_jfk(self):
        """LAX → JFK domestic route ≈ 2,144 NM."""
        d = haversine(33.942, -118.408, 40.641, -73.778)
        assert 2_100 < d < 2_200

    def test_symmetry(self):
        d1 = haversine(35.0, -100.0, 40.0, -95.0)
        d2 = haversine(40.0, -95.0, 35.0, -100.0)
        assert d1 == pytest.approx(d2, rel=1e-9)

    def test_result_positive(self):
        assert haversine(0.0, 0.0, -1.0, -1.0) > 0

    def test_8nm_detection_boundary(self):
        """Two aircraft exactly 8 NM apart (≈ 0.1332° lat) should measure ≈ 8 NM."""
        offset_deg = 8.0 / 60.04
        d = haversine(39.5, -98.35, 39.5 + offset_deg, -98.35)
        assert d == pytest.approx(8.0, rel=0.01)

    def test_antipodal_points(self):
        """Points on opposite sides of Earth ≈ π × R_NM."""
        d = haversine(0.0, 0.0, 0.0, 180.0)
        assert d == pytest.approx(math.pi * EARTH_R_NM, rel=0.001)

    def test_returns_nautical_miles_not_km(self):
        """1° lat ≈ 111 km = 59.9 NM — confirm units are NM, not km."""
        d = haversine(0.0, 0.0, 1.0, 0.0)
        assert d < 100, "result looks like kilometres, not nautical miles"


# ---------------------------------------------------------------------------
# normalize_altitude
# ---------------------------------------------------------------------------


class TestNormalizeAltitude:
    def test_both_present_returns_baro(self):
        """Barometric altitude is preferred when both sources are available."""
        result = normalize_altitude(10_000.0, 9_950.0)
        assert result == pytest.approx(10_000.0)

    def test_baro_only(self):
        assert normalize_altitude(8_000.0, None) == pytest.approx(8_000.0)

    def test_geo_only(self):
        assert normalize_altitude(None, 7_500.0) == pytest.approx(7_500.0)

    def test_neither_returns_none(self):
        assert normalize_altitude(None, None) is None

    def test_zero_altitude_is_valid(self):
        """Ground level is a legitimate altitude."""
        assert normalize_altitude(0.0, 0.0) == pytest.approx(0.0)

    def test_negative_altitude_is_valid(self):
        """Below-MSL altitudes occur near Death Valley, etc."""
        assert normalize_altitude(-86.0, None) == pytest.approx(-86.0)

    def test_ignores_geo_when_baro_present(self):
        """Large baro/geo divergence — baro still wins."""
        assert normalize_altitude(35_000.0, 34_000.0) == pytest.approx(35_000.0)


# ---------------------------------------------------------------------------
# vertical_separation
# ---------------------------------------------------------------------------


class TestVerticalSeparation:
    def test_standard_separation(self):
        assert vertical_separation(35_000.0, 33_000.0) == pytest.approx(2_000.0)

    def test_symmetric(self):
        """Order of aircraft A and B must not affect the result."""
        assert vertical_separation(33_000.0, 35_000.0) == pytest.approx(2_000.0)

    def test_same_altitude(self):
        assert vertical_separation(30_000.0, 30_000.0) == pytest.approx(0.0)

    def test_first_none_returns_none(self):
        assert vertical_separation(None, 30_000.0) is None

    def test_second_none_returns_none(self):
        assert vertical_separation(30_000.0, None) is None

    def test_both_none_returns_none(self):
        assert vertical_separation(None, None) is None

    def test_result_always_non_negative(self):
        for a, b in [(35_000, 33_000), (33_000, 35_000), (30_000, 30_000)]:
            assert vertical_separation(float(a), float(b)) >= 0.0


# ---------------------------------------------------------------------------
# closure_rate (magnitude-based, no positions)
# ---------------------------------------------------------------------------


class TestClosureRate:
    def test_returns_none_when_any_input_none(self):
        assert closure_rate(None,  90.0, 250.0, 270.0) is None
        assert closure_rate(250.0, None, 250.0, 270.0) is None
        assert closure_rate(250.0,  90.0, None,  270.0) is None
        assert closure_rate(250.0,  90.0, 250.0, None) is None

    def test_returns_float_when_all_present(self):
        result = closure_rate(250.0, 90.0, 250.0, 270.0)
        assert isinstance(result, float)
        assert result >= 0.0

    def test_parallel_same_direction_is_zero(self):
        """Two aircraft flying the same course at the same speed: no relative motion."""
        result = closure_rate(250.0, 90.0, 250.0, 90.0)
        assert result == pytest.approx(0.0, abs=0.1)

    def test_opposite_directions_large_value(self):
        """Head-on: relative speed is double the individual speed."""
        result = closure_rate(250.0, 90.0, 250.0, 270.0)
        assert result is not None
        # 2 × 250 m/s × MS_TO_KTS ≈ 972 kts
        expected_kts = 2 * 250.0 / 0.514444
        assert result == pytest.approx(expected_kts, rel=0.01)


# ---------------------------------------------------------------------------
# closure_rate_with_positions
# ---------------------------------------------------------------------------


class TestClosureRateWithPositions:
    def test_head_on_converging_is_positive(self):
        """A at (0,0) flying east, B at (0,0.5) flying west → closing."""
        rate = closure_rate_with_positions(
            0.0, 0.0, 250.0, 90.0,
            0.0, 0.5, 250.0, 270.0,
        )
        assert rate is not None
        assert rate > 0, f"expected positive closure, got {rate}"

    def test_diverging_is_negative(self):
        """A flying west away from B, B flying east away from A."""
        rate = closure_rate_with_positions(
            0.0, 0.0, 250.0, 270.0,
            0.0, 0.5, 250.0,  90.0,
        )
        assert rate is not None
        assert rate < 0, f"expected negative closure, got {rate}"

    def test_missing_velocity_returns_none(self):
        rate = closure_rate_with_positions(
            0.0, 0.0, None, 90.0,
            0.0, 0.5, 250.0, 270.0,
        )
        assert rate is None

    def test_missing_track_returns_none(self):
        rate = closure_rate_with_positions(
            0.0, 0.0, 250.0, None,
            0.0, 0.5, 250.0, 270.0,
        )
        assert rate is None

    def test_coincident_positions_returns_zero(self):
        """Aircraft at the same position: no separation vector → 0 closure."""
        rate = closure_rate_with_positions(
            0.0, 0.0, 250.0, 90.0,
            0.0, 0.0, 250.0, 270.0,
        )
        assert rate == pytest.approx(0.0, abs=0.1)

    def test_perpendicular_tracks_closure_less_than_head_on(self):
        """90° crossing: closure ≤ head-on closure rate."""
        head_on = closure_rate_with_positions(
            0.0, 0.0, 250.0,  0.0,
            0.0, 0.5, 250.0, 180.0,
        )
        crossing = closure_rate_with_positions(
            0.0, 0.0, 250.0,  90.0,
            0.0, 0.5, 250.0, 180.0,
        )
        assert head_on is not None and crossing is not None
        assert abs(crossing) <= abs(head_on) + 1.0  # small tolerance for floating-point


# ---------------------------------------------------------------------------
# classify_separation
# ---------------------------------------------------------------------------


class TestClassifySeparation:
    # --- NORMAL ---

    def test_normal_far_horizontal(self):
        assert classify_separation(20.0, 5_000.0) == SeparationLevel.NORMAL

    def test_normal_close_horizontal_large_vertical(self):
        """Inside 8 NM but vertical separation is large → NORMAL."""
        assert classify_separation(2.0, 2_500.0) == SeparationLevel.NORMAL

    def test_normal_exactly_8nm_boundary(self):
        """Exactly 8 NM is NOT inside the proximity threshold."""
        assert classify_separation(8.0, 1_000.0) == SeparationLevel.NORMAL

    def test_normal_large_vertical_at_5nm(self):
        assert classify_separation(4.9, 2_000.0) == SeparationLevel.NORMAL

    # --- PROXIMITY ---

    def test_proximity_classic(self):
        assert classify_separation(7.0, 1_800.0) == SeparationLevel.PROXIMITY

    def test_proximity_just_inside_boundary(self):
        assert classify_separation(7.9, 1_999.0) == SeparationLevel.PROXIMITY

    def test_proximity_not_triggered_by_large_vertical(self):
        assert classify_separation(7.0, 2_000.0) == SeparationLevel.NORMAL

    # --- NEAR MISS ---

    def test_near_miss_classic(self):
        assert classify_separation(4.0, 1_200.0) == SeparationLevel.NEAR_MISS

    def test_near_miss_just_inside_boundary(self):
        assert classify_separation(4.9, 1_499.0) == SeparationLevel.NEAR_MISS

    def test_near_miss_not_triggered_by_large_vertical(self):
        assert classify_separation(4.0, 1_500.0) == SeparationLevel.PROXIMITY

    # --- LOSS OF SEPARATION ---

    def test_los_classic(self):
        assert classify_separation(1.5, 500.0) == SeparationLevel.LOSS_OF_SEPARATION

    def test_los_just_inside_boundary(self):
        assert classify_separation(2.9, 999.0) == SeparationLevel.LOSS_OF_SEPARATION

    def test_los_not_triggered_large_vertical(self):
        """Inside 3 NM but vertical sep ≥ 1 000 ft → demotes to NEAR_MISS."""
        assert classify_separation(2.0, 1_000.0) == SeparationLevel.NEAR_MISS

    # --- UNKNOWN (no altitude data) ---

    def test_unknown_close_no_altitude(self):
        """< 5 NM, no altitude → conservatively flag as UNKNOWN."""
        assert classify_separation(3.0, None) == SeparationLevel.UNKNOWN

    def test_unknown_very_close_no_altitude(self):
        assert classify_separation(0.5, None) == SeparationLevel.UNKNOWN

    def test_normal_far_no_altitude(self):
        """≥ 5 NM, no altitude → NORMAL (no proximity concern)."""
        assert classify_separation(6.0, None) == SeparationLevel.NORMAL

    def test_normal_exactly_5nm_no_altitude(self):
        assert classify_separation(5.0, None) == SeparationLevel.NORMAL

    # --- Terminal airspace override ---

    def test_terminal_los_inside_1nm_500ft(self):
        """Class B: < 1 NM AND < 500 ft → explicit terminal LoS check."""
        assert (
            classify_separation(0.8, 400.0, airspace_class="B")
            == SeparationLevel.LOSS_OF_SEPARATION
        )

    def test_terminal_class_c(self):
        assert (
            classify_separation(0.8, 400.0, airspace_class="C")
            == SeparationLevel.LOSS_OF_SEPARATION
        )

    def test_enroute_default(self):
        """Default airspace class 'E' uses only en-route thresholds."""
        assert classify_separation(2.0, 800.0) == SeparationLevel.LOSS_OF_SEPARATION

    # --- Parametrized table ---

    @pytest.mark.parametrize(
        "horiz, vert, expected",
        [
            (0.5,  200.0,  SeparationLevel.LOSS_OF_SEPARATION),
            (1.5,  800.0,  SeparationLevel.LOSS_OF_SEPARATION),
            (3.5, 1_200.0, SeparationLevel.NEAR_MISS),
            (4.9, 1_400.0, SeparationLevel.NEAR_MISS),
            (5.5, 1_800.0, SeparationLevel.PROXIMITY),
            (7.5, 1_900.0, SeparationLevel.PROXIMITY),
            (8.1, 1_000.0, SeparationLevel.NORMAL),
            (5.0, 1_600.0, SeparationLevel.PROXIMITY),  # exactly 5 NM: NM threshold exclusive
            (3.0,   900.0, SeparationLevel.NEAR_MISS),  # exactly 3 NM: LoS threshold exclusive
        ],
    )
    def test_parametrized_classification(self, horiz, vert, expected):
        assert classify_separation(horiz, vert) == expected


# ---------------------------------------------------------------------------
# Unit constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_earth_radius_reasonable(self):
        """Earth radius in NM should be ~3 440 NM."""
        assert 3_430 < EARTH_R_NM < 3_450

    def test_m_to_ft_conversion(self):
        """1 metre = 3.28084 feet."""
        assert M_TO_FT == pytest.approx(3.28084, rel=1e-4)

    def test_separation_level_values(self):
        for level in SeparationLevel:
            assert isinstance(level.value, str)
