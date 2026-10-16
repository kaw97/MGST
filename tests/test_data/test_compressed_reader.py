"""Tests for compressed_reader.py — transparent gzip support."""

import gzip
import json

import pytest

from mgst.data.compressed_reader import CompressedFileReader


@pytest.fixture
def plain_file(tmp_path):
    """Create a plain text JSONL file."""
    path = tmp_path / "data.jsonl"
    lines = [
        json.dumps({"name": "Sol", "id": 1}),
        json.dumps({"name": "Alpha Centauri", "id": 2}),
        json.dumps({"name": "Wolf 359", "id": 3}),
    ]
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.fixture
def compressed_file(tmp_path):
    """Create a gzip-compressed JSONL file."""
    path = tmp_path / "data.jsonl.gz"
    lines = [
        json.dumps({"name": "Sol", "id": 1}),
        json.dumps({"name": "Alpha Centauri", "id": 2}),
        json.dumps({"name": "Wolf 359", "id": 3}),
    ]
    content = ("\n".join(lines) + "\n").encode("utf-8")
    with gzip.open(path, "wb") as f:
        f.write(content)
    return path


class TestReadFiles:
    def test_read_uncompressed_file(self, plain_file):
        with CompressedFileReader(plain_file) as f:
            content = f.read()
        lines = [l for l in content.strip().split("\n") if l]
        assert len(lines) == 3
        assert json.loads(lines[0])["name"] == "Sol"

    def test_read_compressed_file(self, compressed_file):
        with CompressedFileReader(compressed_file) as f:
            content = f.read()
        lines = [l for l in content.strip().split("\n") if l]
        assert len(lines) == 3
        assert json.loads(lines[0])["name"] == "Sol"


class TestCompressionDetection:
    def test_detect_by_extension(self, compressed_file):
        reader = CompressedFileReader(compressed_file)
        assert reader._detect_compression() is True

    def test_detect_by_magic_bytes(self, tmp_path):
        """Detect gzip even without .gz extension."""
        path = tmp_path / "data.dat"
        content = b"test content"
        with gzip.open(path, "wb") as f:
            f.write(content)
        reader = CompressedFileReader(path)
        assert reader._detect_compression() is True

    def test_detect_uncompressed(self, plain_file):
        reader = CompressedFileReader(plain_file)
        assert reader._detect_compression() is False


class TestErrorHandling:
    def test_file_not_found_raises(self, tmp_path):
        reader = CompressedFileReader(tmp_path / "nonexistent.jsonl")
        with pytest.raises(FileNotFoundError):
            reader.open()

    def test_read_without_open_raises(self, plain_file):
        reader = CompressedFileReader(plain_file)
        with pytest.raises(ValueError, match="not open"):
            reader.read()


class TestContextManager:
    def test_context_manager_opens_and_closes(self, plain_file):
        reader = CompressedFileReader(plain_file)
        with reader as f:
            assert f.file_handle is not None
        assert reader.file_handle is None


class TestCompressionInfo:
    def test_compression_info_compressed(self, compressed_file):
        with CompressedFileReader(compressed_file) as f:
            info = f.get_compression_info()
        assert info["is_compressed"] is True
        assert info["compressed_size"] > 0

    def test_compression_info_uncompressed(self, plain_file):
        with CompressedFileReader(plain_file) as f:
            info = f.get_compression_info()
        assert info["is_compressed"] is False


class TestReadline:
    def test_readline(self, plain_file):
        with CompressedFileReader(plain_file) as f:
            line1 = f.readline()
            line2 = f.readline()
        assert json.loads(line1)["name"] == "Sol"
        assert json.loads(line2)["name"] == "Alpha Centauri"

    def test_readline_compressed(self, compressed_file):
        with CompressedFileReader(compressed_file) as f:
            line1 = f.readline()
        assert json.loads(line1)["name"] == "Sol"


class TestStreamingChunks:
    def test_chunks_produce_same_content(self, plain_file):
        """Reading in small chunks produces same content as full read."""
        with CompressedFileReader(plain_file) as f:
            full = f.read()

        chunks = []
        with CompressedFileReader(plain_file) as f:
            while True:
                chunk = f.read(20)
                if not chunk:
                    break
                chunks.append(chunk)

        assert "".join(chunks) == full
