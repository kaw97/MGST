"""Tests for JSONPatternConfig — the config wrapper around the pattern matcher."""

import json
import os
import tempfile

import pytest

from mgst.configs.json_pattern import JSONPatternConfig


@pytest.fixture
def pattern_file(tmp_path):
    """Create a temporary pattern file."""
    pattern = {
        "description": "Test pattern - ELW with rocky moon",
        "name": "*",
        "bodies": [
            {"comment": "ELW", "bodyId": "$planet", "subType": "Earth-like world"},
            {"comment": "Rocky moon", "subType": "Rocky body", "parents": [{"Planet": "$planet"}]},
        ],
    }
    path = tmp_path / "test_pattern.json"
    path.write_text(json.dumps(pattern))
    return path


@pytest.fixture
def matching_system():
    return {
        "name": "Test System",
        "id64": 999,
        "coords": {"x": 100.0, "y": -50.0, "z": 200.0},
        "bodies": [
            {"bodyId": 5, "subType": "Earth-like world", "parents": [], "rings": []},
            {"bodyId": 7, "subType": "Rocky body", "parents": [{"Planet": 5}], "rings": []},
        ],
    }


@pytest.fixture
def non_matching_system():
    return {
        "name": "Empty System",
        "id64": 888,
        "coords": {"x": 0.0, "y": 0.0, "z": 0.0},
        "bodies": [
            {"bodyId": 1, "subType": "Gas giant", "parents": [], "rings": []},
        ],
    }


class TestLoadPattern:
    def test_load_pattern_from_file(self, pattern_file):
        config = JSONPatternConfig(pattern_file)
        assert config.pattern is not None
        assert config.pattern["name"] == "*"
        assert "ELW with rocky moon" in config.get_description()


class TestFilterSystem:
    def test_filter_system_matching(self, pattern_file, matching_system):
        config = JSONPatternConfig(pattern_file)
        result = config.filter_system(matching_system)
        assert result is not None
        assert result["name"] == "Test System"

    def test_filter_system_no_match(self, pattern_file, non_matching_system):
        config = JSONPatternConfig(pattern_file)
        result = config.filter_system(non_matching_system)
        assert result is None

    def test_result_contains_coordinates(self, pattern_file, matching_system):
        config = JSONPatternConfig(pattern_file)
        result = config.filter_system(matching_system)
        assert result["x"] == 100.0
        assert result["y"] == -50.0
        assert result["z"] == 200.0

    def test_result_body_type_counts(self, pattern_file, matching_system):
        config = JSONPatternConfig(pattern_file)
        result = config.filter_system(matching_system)
        assert result["Earth-like world_count"] == 1
        assert result["Rocky body_count"] == 1
        assert result["total_bodies"] == 2


class TestCorridorFiltering:
    def test_filter_system_with_corridor_rejects_distant(self, pattern_file, matching_system):
        corridor = {
            "start_coords": (0.0, 0.0, 0.0),
            "end_coords": (10.0, 0.0, 0.0),
            "radius": 5.0,
        }
        config = JSONPatternConfig(pattern_file, corridor_params=corridor)
        # System at (100, -50, 200) is far from the corridor
        result = config.filter_system(matching_system)
        assert result is None

    def test_filter_system_corridor_accepts_nearby(self, pattern_file, matching_system):
        corridor = {
            "start_coords": (0.0, 0.0, 0.0),
            "end_coords": (200.0, -100.0, 400.0),
            "radius": 500.0,
        }
        config = JSONPatternConfig(pattern_file, corridor_params=corridor)
        result = config.filter_system(matching_system)
        assert result is not None
        assert "corridor_distance" in result


class TestOutputColumns:
    def test_output_columns_basic(self, pattern_file):
        config = JSONPatternConfig(pattern_file)
        cols = config.get_output_columns()
        assert "name" in cols
        assert "x" in cols
        assert "y" in cols
        assert "z" in cols
        assert "total_bodies" in cols

    def test_output_columns_with_corridor(self, pattern_file):
        corridor = {
            "start_coords": (0, 0, 0),
            "end_coords": (10, 0, 0),
            "radius": 100,
        }
        config = JSONPatternConfig(pattern_file, corridor_params=corridor)
        cols = config.get_output_columns()
        assert "corridor_distance" in cols
