"""Tests for Parquet schema, writer, reader, and filtering modules."""

import json
import pytest
from pathlib import Path

import pyarrow.parquet as pq

from mgst.data.parquet_schema import (
    SchemaDiscoverer,
    flatten_body,
    flatten_station,
    flatten_system,
    unflatten_body,
    unflatten_station,
    unflatten_system,
    unflatten_system_with_bodies,
)
from mgst.data.parquet_writer import ParquetSectorWriter
from mgst.data.parquet_reader import (
    ParquetSectorReader,
    detect_parquet_database,
    get_sector_readers,
)
from mgst.core.pattern_compiler import PatternCompiler
from mgst.core.json_pattern_matcher import JSONPatternMatcher


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_star():
    """A sample star body dict."""
    return {
        "bodyId": 0,
        "id64": 100,
        "name": "Test System A",
        "type": "Star",
        "subType": "K (Yellow-Orange) Star",
        "distanceToArrival": 0.0,
        "mainStar": True,
        "age": 6000,
        "luminosity": "Va",
        "absoluteMagnitude": 7.5,
        "solarMasses": 0.75,
        "solarRadius": 0.8,
        "surfaceTemperature": 4500,
        "rotationalPeriod": 10.5,
        "rotationalPeriodTidallyLocked": False,
        "axialTilt": 0.0,
        "spectralClass": "K3",
        "updateTime": "2024-01-01T00:00:00Z",
        "timestamps": {"lastUpdated": "2024-01-01"},
        "stations": [],
    }


@pytest.fixture
def sample_planet():
    """A sample planet body dict with rich nested data."""
    return {
        "bodyId": 1,
        "id64": 101,
        "name": "Test System A 1",
        "type": "Planet",
        "subType": "Earth-like world",
        "distanceToArrival": 500.0,
        "isLandable": False,
        "gravity": 1.0,
        "earthMasses": 1.0,
        "radius": 6371.0,
        "surfaceTemperature": 288.0,
        "surfacePressure": 1.0,
        "atmosphereType": "Suitable for water-based life",
        "atmosphereComposition": {
            "Nitrogen": 78.08,
            "Oxygen": 20.95,
            "Argon": 0.93,
        },
        "solidComposition": {"Rock": 70.0, "Metal": 30.0},
        "terraformingState": "Terraformable",
        "volcanismType": "No volcanism",
        "orbitalPeriod": 365.25,
        "semiMajorAxis": 1.0,
        "orbitalEccentricity": 0.017,
        "orbitalInclination": 0.0,
        "argOfPeriapsis": 102.9,
        "rotationalPeriod": 1.0,
        "rotationalPeriodTidallyLocked": False,
        "axialTilt": 23.44,
        "updateTime": "2024-01-01T00:00:00Z",
        "parents": [{"Star": 0}],
        "rings": [
            {
                "name": "Test Ring",
                "type": "Rocky",
                "mass": 1e10,
                "innerRadius": 7000.0,
                "outerRadius": 10000.0,
            }
        ],
        "signals": {
            "signals": {"$SAA_SignalType_Biological;": 3},
            "updateTime": "2024-01-01T00:00:00Z",
            "genuses": [{"genus": "Bacterium", "count": 2}],
        },
        "timestamps": {"lastScanned": "2024-01-01"},
        "stations": [],
    }


@pytest.fixture
def sample_station():
    """A sample station dict."""
    return {
        "id": 1000,
        "name": "Test Station",
        "type": "Coriolis Starport",
        "distanceToArrival": 100.0,
        "allegiance": "Federation",
        "government": "Democracy",
        "economy": "High Tech",
        "controllingFaction": "Test Faction",
        "controllingFactionState": None,
        "landingPads": {"large": 10, "medium": 15, "small": 20},
        "economies": [{"name": "High Tech", "proportion": 0.8}],
        "services": ["Dock", "Refuel", "Repair"],
    }


@pytest.fixture
def sample_system(sample_star, sample_planet, sample_station):
    """A complete sample system dict."""
    return {
        "id64": 12345678,
        "name": "Test System",
        "coords": {"x": 100.5, "y": -200.0, "z": 300.25},
        "date": "2024-01-01",
        "population": 0,
        "allegiance": None,
        "government": None,
        "bodies": [sample_star, sample_planet],
        "stations": [sample_station],
    }


# ---------------------------------------------------------------------------
# Schema discovery tests
# ---------------------------------------------------------------------------

class TestSchemaDiscoverer:
    def test_observe_system_tracks_columns(self, sample_system):
        discoverer = SchemaDiscoverer()
        discoverer.observe_system(sample_system)

        assert "id64" in discoverer.system_columns
        assert "name" in discoverer.system_columns
        assert "x" in discoverer.system_columns
        assert "system_id64" in discoverer.body_columns
        assert "subType" in discoverer.body_columns

    def test_observe_system_tracks_nested_fields(self, sample_system):
        discoverer = SchemaDiscoverer()
        discoverer.observe_system(sample_system)

        # Materials should be discovered as prefixed columns
        assert any(c.startswith("atm_") for c in discoverer.body_columns)
        assert any(c.startswith("solid_") for c in discoverer.body_columns)

    def test_systems_observed_counter(self, sample_system):
        discoverer = SchemaDiscoverer()
        discoverer.observe_system(sample_system)
        discoverer.observe_system(sample_system)
        assert discoverer.systems_observed == 2


# ---------------------------------------------------------------------------
# Flatten/unflatten roundtrip tests
# ---------------------------------------------------------------------------

class TestFlattenUnflatten:
    def _normalize(self, obj):
        """Normalize for comparison, stripping None values."""
        if isinstance(obj, dict):
            return {k: self._normalize(v) for k, v in sorted(obj.items())
                    if v is not None}
        if isinstance(obj, list):
            return [self._normalize(v) for v in obj]
        if isinstance(obj, float):
            return round(obj, 10)
        return obj

    def test_system_roundtrip(self, sample_system):
        flat = flatten_system(sample_system)
        recon = unflatten_system(flat)

        # System roundtrip should preserve coords
        assert recon["coords"]["x"] == sample_system["coords"]["x"]
        assert recon["name"] == sample_system["name"]
        assert recon["id64"] == sample_system["id64"]

    def test_body_star_roundtrip(self, sample_star):
        flat = flatten_body(sample_star, 999)
        recon = unflatten_body(flat)

        assert recon["type"] == "Star"
        assert recon["subType"] == sample_star["subType"]
        assert recon["stations"] == []

    def test_body_planet_roundtrip(self, sample_planet):
        flat = flatten_body(sample_planet, 999)
        recon = unflatten_body(flat)

        norm_orig = self._normalize(sample_planet)
        norm_recon = self._normalize(recon)
        assert norm_orig == norm_recon

    def test_body_atmosphere_roundtrip(self, sample_planet):
        flat = flatten_body(sample_planet, 999)
        recon = unflatten_body(flat)

        assert "atmosphereComposition" in recon
        assert recon["atmosphereComposition"]["Nitrogen"] == 78.08

    def test_body_parents_roundtrip(self, sample_planet):
        flat = flatten_body(sample_planet, 999)
        recon = unflatten_body(flat)

        assert recon["parents"] == [{"Star": 0}]

    def test_body_rings_roundtrip(self, sample_planet):
        flat = flatten_body(sample_planet, 999)
        recon = unflatten_body(flat)

        assert len(recon["rings"]) == 1
        assert recon["rings"][0]["type"] == "Rocky"

    def test_body_signals_roundtrip(self, sample_planet):
        flat = flatten_body(sample_planet, 999)
        recon = unflatten_body(flat)

        assert "signals" in recon
        assert recon["signals"]["signals"]["$SAA_SignalType_Biological;"] == 3

    def test_station_roundtrip(self, sample_station):
        flat = flatten_station(sample_station, 999)
        recon = unflatten_station(flat)

        assert recon["name"] == "Test Station"
        assert recon["landingPads"]["large"] == 10
        assert recon["economies"] == sample_station["economies"]
        # controllingFactionState is None — preserved through direct roundtrip
        assert recon["controllingFactionState"] is None

    def test_full_system_roundtrip(self, sample_system):
        sid = sample_system["id64"]
        flat_sys = flatten_system(sample_system)
        flat_bodies = [flatten_body(b, sid) for b in sample_system["bodies"]]
        flat_stations = [flatten_station(s, sid) for s in sample_system["stations"]]

        recon = unflatten_system_with_bodies(flat_sys, flat_bodies, flat_stations)

        norm_orig = self._normalize(sample_system)
        norm_recon = self._normalize(recon)
        assert norm_orig == norm_recon


# ---------------------------------------------------------------------------
# Writer/Reader tests
# ---------------------------------------------------------------------------

class TestParquetWriterReader:
    def test_write_and_read(self, tmp_path, sample_system):
        writer = ParquetSectorWriter(tmp_path, flush_threshold=10)
        writer.add_system("TestSector", sample_system)
        writer.finalize()

        sector_dir = tmp_path / "TestSector"
        assert (sector_dir / "systems.parquet").exists()
        assert (sector_dir / "bodies.parquet").exists()
        assert (sector_dir / "stations.parquet").exists()

    def test_system_count(self, tmp_path, sample_system):
        writer = ParquetSectorWriter(tmp_path, flush_threshold=100)
        for _ in range(5):
            writer.add_system("TestSector", sample_system)
        writer.finalize()

        reader = ParquetSectorReader(tmp_path / "TestSector")
        assert reader.system_count() == 5

    def test_column_pruning(self, tmp_path, sample_system):
        writer = ParquetSectorWriter(tmp_path, flush_threshold=100)
        writer.add_system("TestSector", sample_system)
        writer.finalize()

        reader = ParquetSectorReader(tmp_path / "TestSector")
        table = reader.read_systems(columns=["id64", "name"])
        assert set(table.column_names) == {"id64", "name"}

    def test_reconstruct_system_dicts(self, tmp_path, sample_system):
        writer = ParquetSectorWriter(tmp_path, flush_threshold=100)
        writer.add_system("TestSector", sample_system)
        writer.finalize()

        reader = ParquetSectorReader(tmp_path / "TestSector")
        systems = reader.reconstruct_system_dicts()
        assert len(systems) == 1
        assert systems[0]["name"] == "Test System"
        assert len(systems[0]["bodies"]) == 2
        assert len(systems[0]["stations"]) == 1


# ---------------------------------------------------------------------------
# Database detection tests
# ---------------------------------------------------------------------------

class TestDatabaseDetection:
    def test_detect_parquet(self, tmp_path, sample_system):
        writer = ParquetSectorWriter(tmp_path, flush_threshold=100)
        writer.add_system("TestSector", sample_system)
        writer.finalize()

        assert detect_parquet_database(tmp_path) is True

    def test_detect_non_parquet(self, tmp_path):
        # Create a fake JSONL file
        (tmp_path / "test.jsonl.gz").touch()
        assert detect_parquet_database(tmp_path) is False

    def test_get_sector_readers(self, tmp_path, sample_system):
        writer = ParquetSectorWriter(tmp_path, flush_threshold=100)
        writer.add_system("SectorA", sample_system)
        writer.add_system("SectorB", sample_system)
        writer.finalize()

        readers = get_sector_readers(tmp_path)
        assert len(readers) == 2


# ---------------------------------------------------------------------------
# Pattern compiler tests
# ---------------------------------------------------------------------------

class TestPatternCompiler:
    def setup_method(self):
        self.compiler = PatternCompiler()

    def test_all_systems_pattern(self):
        compiled = self.compiler.compile({"name": "*"})
        assert compiled.combined_system_filter() is None
        assert not compiled.needs_full_reconstruction

    def test_exact_match_pattern(self):
        compiled = self.compiler.compile({"name": "*", "population": 0})
        assert compiled.combined_system_filter() is not None
        assert not compiled.needs_full_reconstruction

    def test_range_pattern(self):
        compiled = self.compiler.compile({"name": "*", "population": {"min": 1}})
        assert compiled.combined_system_filter() is not None
        assert "population" in compiled.system_columns

    def test_list_with_null(self):
        compiled = self.compiler.compile({"name": "*", "population": [0, None]})
        assert compiled.combined_system_filter() is not None

    def test_body_type_pushdown(self):
        compiled = self.compiler.compile({
            "name": "*",
            "bodies": [{"type": "Planet", "subType": "Earth-like world"}],
        })
        assert len(compiled.body_entry_filters) == 1
        assert compiled.combined_body_filter(0) is not None
        assert not compiled.needs_full_reconstruction

    def test_variable_binding_requires_reconstruction(self):
        compiled = self.compiler.compile({
            "name": "*",
            "bodies": [
                {"bodyId": "$planet", "subType": "Earth-like world"},
                {"parents": [{"Planet": "$planet"}]},
            ],
        })
        assert compiled.needs_full_reconstruction

    def test_parent_matching_requires_reconstruction(self):
        compiled = self.compiler.compile({
            "name": "*",
            "bodies": [{"parents": [{"Star": "*"}]}],
        })
        assert compiled.needs_full_reconstruction

    def test_glob_name_requires_reconstruction(self):
        compiled = self.compiler.compile({"name": "*ME-R*"})
        assert compiled.needs_full_reconstruction

    def test_controlling_faction_pushdown(self):
        compiled = self.compiler.compile({
            "name": "*",
            "controllingFaction": {"name": "Test Faction"},
        })
        assert "controllingFaction_name" in compiled.system_columns
        assert not compiled.needs_full_reconstruction


# ---------------------------------------------------------------------------
# End-to-end filtering test
# ---------------------------------------------------------------------------

class TestParquetFiltering:
    def _normalize(self, obj):
        if isinstance(obj, dict):
            return {k: self._normalize(v) for k, v in sorted(obj.items())
                    if v is not None}
        if isinstance(obj, list):
            return [self._normalize(v) for v in obj]
        if isinstance(obj, float):
            return round(obj, 10)
        return obj

    def test_simple_pattern_equivalence(self, tmp_path, sample_system):
        """Verify Parquet filtering produces same results as direct matching."""
        from mgst.core.parquet_filtering import process_parquet_sector
        from mgst.configs.json_pattern import JSONPatternConfig

        # Create a system that matches ELW pattern
        writer = ParquetSectorWriter(tmp_path, flush_threshold=100)
        writer.add_system("TestSector", sample_system)
        writer.finalize()

        pattern = {
            "name": "*",
            "population": [0, None],
            "bodies": [{"type": "Planet", "subType": "Earth-like world"}],
        }

        # Direct matcher
        matcher = JSONPatternMatcher(pattern)
        matcher.reset()
        direct_match = matcher.matches(sample_system)

        # Parquet path
        compiled = PatternCompiler().compile(pattern)

        # Write pattern to temp file for JSONPatternConfig
        pattern_file = tmp_path / "test_pattern.json"
        with open(pattern_file, "w") as f:
            json.dump(pattern, f)

        config = JSONPatternConfig(pattern_file)
        result = process_parquet_sector(
            tmp_path / "TestSector", pattern, compiled, config, "", "jsonl", False
        )

        parquet_match = result["matches_found"] > 0
        assert direct_match == parquet_match
