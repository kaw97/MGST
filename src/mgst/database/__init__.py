"""High-performance time-series galaxy database system."""

# Submodules are available for import but not loaded at package level
# This prevents circular import issues during installation
# Import submodules explicitly when needed:
#   from mgst.database.builder import GalaxyDatabaseBuilder
#   from mgst.database.updater import GalaxyDatabaseUpdater
#   from mgst.database.schema import TimeSeriesRecord

__all__ = [
    "builder",
    "change_detector",
    "downloader",
    "schema",
    "updater",
]