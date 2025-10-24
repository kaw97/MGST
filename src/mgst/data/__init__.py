"""Data processing and conversion utilities."""

# Submodules are available for import but not loaded at package level
# This prevents circular import issues during installation
# Import submodules explicitly when needed:
#   from mgst.data.loaders import load_system_data
#   from mgst.data.compressed_reader import CompressedFileReader
#   from mgst.data.validators import validate_system_data

__all__ = [
    "compressed_reader",
    "converters",
    "indexed_reader",
    "loaders",
    "validators",
]