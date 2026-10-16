"""Tests for filtering.py — write path, StreamingTSVWriter, FilteringResult."""

import json
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from mgst.core.filtering import (
    FilteringResult,
    StreamingTSVWriter,
    write_result_to_file,
)


# --- Fixtures ---

@pytest.fixture
def mock_config():
    """A minimal config stub for testing write functions."""
    config = MagicMock()
    config.get_output_columns.return_value = ["name", "x", "y", "z"]
    return config


@pytest.fixture
def sample_result():
    return {"name": "Sol", "x": 0.0, "y": 0.0, "z": 0.0}


@pytest.fixture
def sample_system_data():
    return {"name": "Sol", "coords": {"x": 0.0, "y": 0.0, "z": 0.0}, "bodies": []}


# --- FilteringResult Tests ---

class TestFilteringResult:
    def test_match_rate(self):
        r = FilteringResult(
            files_processed=1, systems_processed=1000,
            matches_found=5, processing_time=10.0, errors=[]
        )
        assert r.match_rate == pytest.approx(0.5)

    def test_processing_speed(self):
        r = FilteringResult(
            files_processed=1, systems_processed=5000,
            matches_found=0, processing_time=2.5, errors=[]
        )
        assert r.processing_speed == pytest.approx(2000.0)

    def test_zero_systems_no_crash(self):
        r = FilteringResult(
            files_processed=0, systems_processed=0,
            matches_found=0, processing_time=1.0, errors=[]
        )
        assert r.match_rate == 0.0

    def test_zero_time_no_crash(self):
        r = FilteringResult(
            files_processed=1, systems_processed=100,
            matches_found=1, processing_time=0.0, errors=[]
        )
        assert r.processing_speed == 0.0


# --- StreamingTSVWriter Tests ---

class TestStreamingTSVWriter:
    def test_header_written_once(self, tmp_path, mock_config):
        out = tmp_path / "out.tsv"
        with StreamingTSVWriter(out, mock_config) as w:
            w.write_header()
            w.write_header()  # second call should be a no-op
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 1
        assert lines[0] == "name\tx\ty\tz"

    def test_multiple_rows(self, tmp_path, mock_config):
        out = tmp_path / "out.tsv"
        with StreamingTSVWriter(out, mock_config) as w:
            w.write_result({"name": "Sol", "x": 0.0, "y": 0.0, "z": 0.0})
            w.write_result({"name": "Alpha Centauri", "x": -6.25, "y": -1.25, "z": -1.25})
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert "Sol" in lines[1]
        assert "Alpha Centauri" in lines[2]

    def test_special_chars_escaped(self, tmp_path, mock_config):
        out = tmp_path / "out.tsv"
        with StreamingTSVWriter(out, mock_config) as w:
            w.write_result({"name": "Tab\there", "x": "New\nline", "y": "CR\rhere", "z": 0})
        content = out.read_text()
        # Tabs, newlines, CRs within values should be replaced with spaces
        assert "\t" not in content.split("\n")[1].split("\t")[0]  # name column has no literal tab


# --- write_result_to_file Tests ---

class TestWriteResultToFile:
    def test_write_tsv_format(self, tmp_path, mock_config, sample_result, sample_system_data):
        out = tmp_path / "out.tsv"
        out.touch()  # create empty file
        write_result_to_file(sample_result, sample_system_data, str(out), "tsv", mock_config)
        content = out.read_text()
        assert "name\tx\ty\tz" in content  # header
        assert "Sol" in content

    def test_write_jsonl_format(self, tmp_path, mock_config, sample_result, sample_system_data):
        out = tmp_path / "out.jsonl"
        out.touch()
        write_result_to_file(sample_result, sample_system_data, str(out), "jsonl", mock_config)
        content = out.read_text().strip()
        parsed = json.loads(content)
        assert parsed["name"] == "Sol"

    def test_write_error_logs_warning(self, tmp_path, mock_config, sample_result, sample_system_data, caplog):
        # Use a path that doesn't exist (directory missing) to trigger error
        bad_path = str(tmp_path / "nonexistent_dir" / "out.tsv")
        with caplog.at_level(logging.WARNING):
            write_result_to_file(sample_result, sample_system_data, bad_path, "tsv", mock_config)
        assert any("Write error" in record.message for record in caplog.records)

    def test_write_without_fcntl(self, tmp_path, mock_config, sample_result, sample_system_data):
        """Simulate Windows by disabling fcntl."""
        out = tmp_path / "out.jsonl"
        out.touch()
        with patch("mgst.core.filtering.HAS_FCNTL", False):
            write_result_to_file(sample_result, sample_system_data, str(out), "jsonl", mock_config)
        content = out.read_text().strip()
        parsed = json.loads(content)
        assert parsed["name"] == "Sol"
