"""Tests for search_modes.py — coordinate parsing, validation, sector resolver."""

import json
from pathlib import Path

import pytest

from mgst.core.search_modes import (
    Coordinates,
    SearchMode,
    SearchParameters,
    SectorResolver,
    parse_coordinates,
    validate_search_parameters,
)


# --- Coordinate Parsing ---

class TestParseCoordinates:
    def test_valid(self):
        c = parse_coordinates("100,-200,300")
        assert c.x == 100.0
        assert c.y == -200.0
        assert c.z == 300.0

    def test_valid_with_decimals(self):
        c = parse_coordinates("100.5,-200.25,300.75")
        assert c.x == 100.5

    def test_invalid_non_numeric(self):
        with pytest.raises(ValueError):
            parse_coordinates("abc,def,ghi")

    def test_wrong_count_two(self):
        with pytest.raises(ValueError):
            parse_coordinates("100,200")

    def test_wrong_count_four(self):
        with pytest.raises(ValueError):
            parse_coordinates("100,200,300,400")

    def test_empty_string(self):
        with pytest.raises(ValueError):
            parse_coordinates("")


# --- Coordinates Methods ---

class TestCoordinates:
    def test_distance_to(self):
        a = Coordinates(0, 0, 0)
        b = Coordinates(3, 4, 0)
        assert a.distance_to(b) == pytest.approx(5.0)

    def test_distance_to_self(self):
        a = Coordinates(10, 20, 30)
        assert a.distance_to(a) == pytest.approx(0.0)

    def test_distance_to_line(self):
        # Point at (5,3,0) perpendicular to segment (0,0,0)-(10,0,0)
        p = Coordinates(5, 3, 0)
        s = Coordinates(0, 0, 0)
        e = Coordinates(10, 0, 0)
        assert p.distance_to_line(s, e) == pytest.approx(3.0)


# --- Validate Search Parameters ---

class TestValidateSearchParameters:
    def test_corridor_missing_start(self, tmp_path):
        db = tmp_path / "db"
        db.mkdir()
        params = SearchParameters(
            mode=SearchMode.CORRIDOR,
            database_path=db,
            start_coords=None,
            end_coords=Coordinates(0, 0, 0),
            radius=100,
        )
        with pytest.raises(ValueError, match="Corridor mode requires"):
            validate_search_parameters(params)

    def test_corridor_negative_radius(self, tmp_path):
        db = tmp_path / "db"
        db.mkdir()
        params = SearchParameters(
            mode=SearchMode.CORRIDOR,
            database_path=db,
            start_coords=Coordinates(0, 0, 0),
            end_coords=Coordinates(10, 0, 0),
            radius=-50,
        )
        with pytest.raises(ValueError, match="radius must be positive"):
            validate_search_parameters(params)

    def test_sectors_missing_sectors(self, tmp_path):
        db = tmp_path / "db"
        db.mkdir()
        params = SearchParameters(
            mode=SearchMode.SECTORS,
            database_path=db,
            sectors=None,
        )
        with pytest.raises(ValueError, match="Sectors mode requires"):
            validate_search_parameters(params)

    def test_pattern_missing_file(self, tmp_path):
        db = tmp_path / "db"
        db.mkdir()
        params = SearchParameters(
            mode=SearchMode.PATTERN,
            database_path=db,
            pattern_file=None,
        )
        with pytest.raises(ValueError, match="Pattern mode requires"):
            validate_search_parameters(params)

    def test_database_not_found(self, tmp_path):
        params = SearchParameters(
            mode=SearchMode.GALAXY,
            database_path=tmp_path / "nonexistent",
        )
        with pytest.raises(ValueError, match="does not exist"):
            validate_search_parameters(params)


# --- Sector Resolver ---

class TestSectorResolver:
    def test_parse_sector_name(self, tmp_path):
        resolver = SectorResolver(tmp_path)
        assert resolver.parse_sector_name("Col_285_Sector.jsonl.gz") == "Col 285 Sector"
        assert resolver.parse_sector_name("Sol_Sector.jsonl") == "Sol Sector"

    def test_parse_sector_name_invalid(self, tmp_path):
        resolver = SectorResolver(tmp_path)
        assert resolver.parse_sector_name("not_a_sector.txt") is None

    def test_resolve_galaxy_returns_all(self, tmp_path):
        # Create some fake sector files
        (tmp_path / "sector_a.jsonl.gz").touch()
        (tmp_path / "sector_b.jsonl.gz").touch()
        resolver = SectorResolver(tmp_path)
        params = SearchParameters(mode=SearchMode.GALAXY, database_path=tmp_path)
        files = resolver.resolve_galaxy_mode(params)
        assert len(files) == 2

    def test_resolve_sectors_filters(self, tmp_path):
        (tmp_path / "Sol_Sector.jsonl.gz").touch()
        (tmp_path / "Alpha_Centauri.jsonl.gz").touch()
        (tmp_path / "Wolf_359.jsonl.gz").touch()
        resolver = SectorResolver(tmp_path)
        params = SearchParameters(
            mode=SearchMode.SECTORS,
            database_path=tmp_path,
            sectors=["Sol Sector", "Wolf 359"],
        )
        files = resolver.resolve_sectors_mode(params)
        names = [f.name for f in files]
        assert "Sol_Sector.jsonl.gz" in names
        assert "Wolf_359.jsonl.gz" in names
        assert "Alpha_Centauri.jsonl.gz" not in names
