"""Tests for math_utils — distance_3d and distance_to_line_segment."""

import math
import pytest

from mgst.utils.math_utils import distance_3d, distance_to_line_segment, distance_to_origin


class TestDistance3d:
    def test_same_point(self):
        assert distance_3d((1, 2, 3), (1, 2, 3)) == 0.0

    def test_known_values(self):
        # 3-4-5 right triangle in x-y plane
        assert distance_3d((0, 0, 0), (3, 4, 0)) == pytest.approx(5.0)

    def test_distance_to_origin(self):
        # Verify distance_to_origin matches distance_3d to (0,0,0)
        p = (3.0, 4.0, 0.0)
        assert distance_to_origin(p) == pytest.approx(distance_3d(p, (0, 0, 0)))

    def test_3d_diagonal(self):
        # sqrt(1 + 1 + 1) = sqrt(3)
        assert distance_3d((0, 0, 0), (1, 1, 1)) == pytest.approx(math.sqrt(3))

    def test_negative_coords(self):
        assert distance_3d((-1, -2, -3), (1, 2, 3)) == pytest.approx(
            math.sqrt(4 + 16 + 36)
        )


class TestDistanceToLineSegment:
    def test_perpendicular_to_midpoint(self):
        """Point perpendicular to the middle of the segment."""
        # Segment along x-axis from (0,0,0) to (10,0,0)
        # Point at (5,3,0) — perpendicular distance is 3
        d = distance_to_line_segment((5, 3, 0), (0, 0, 0), (10, 0, 0))
        assert d == pytest.approx(3.0)

    def test_closest_at_start(self):
        """Point closest to the start of the segment."""
        # Segment from (0,0,0) to (10,0,0), point at (-5,0,0)
        d = distance_to_line_segment((-5, 0, 0), (0, 0, 0), (10, 0, 0))
        assert d == pytest.approx(5.0)

    def test_closest_at_end(self):
        """Point closest to the end of the segment."""
        # Segment from (0,0,0) to (10,0,0), point at (15,0,0)
        d = distance_to_line_segment((15, 0, 0), (0, 0, 0), (10, 0, 0))
        assert d == pytest.approx(5.0)

    def test_zero_length_segment(self):
        """Degenerate segment (start == end) returns point-to-point distance."""
        d = distance_to_line_segment((3, 4, 0), (0, 0, 0), (0, 0, 0))
        assert d == pytest.approx(5.0)

    def test_point_on_segment(self):
        """Point lies exactly on the segment."""
        d = distance_to_line_segment((5, 0, 0), (0, 0, 0), (10, 0, 0))
        assert d == pytest.approx(0.0)

    def test_3d_perpendicular(self):
        """Point perpendicular to segment in full 3D."""
        # Segment along z-axis from (0,0,0) to (0,0,10)
        # Point at (3,4,5) — perpendicular distance is sqrt(9+16)=5
        d = distance_to_line_segment((3, 4, 5), (0, 0, 0), (0, 0, 10))
        assert d == pytest.approx(5.0)

    def test_consistency_with_search_modes(self):
        """Verify that search_modes.Coordinates.distance_to_line gives same result."""
        from mgst.core.search_modes import Coordinates

        point = (100.5, -200.0, 300.0)
        start = (0.0, 0.0, 0.0)
        end = (500.0, -100.0, 600.0)

        d_math = distance_to_line_segment(point, start, end)

        p = Coordinates(*point)
        s = Coordinates(*start)
        e = Coordinates(*end)
        d_coords = p.distance_to_line(s, e)

        assert d_math == pytest.approx(d_coords)

    def test_consistency_with_json_pattern(self):
        """Verify that JSONPatternConfig._distance_to_line_segment gives same result."""
        from mgst.configs.json_pattern import JSONPatternConfig
        import tempfile, json, os

        point = (100.5, -200.0, 300.0)
        start = (0.0, 0.0, 0.0)
        end = (500.0, -100.0, 600.0)

        d_math = distance_to_line_segment(point, start, end)

        # Create a minimal pattern file for the config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"name": "*"}, f)
            tmpfile = f.name

        try:
            config = JSONPatternConfig(tmpfile)
            d_config = config._distance_to_line_segment(point, start, end)
            assert d_math == pytest.approx(d_config)
        finally:
            os.unlink(tmpfile)
