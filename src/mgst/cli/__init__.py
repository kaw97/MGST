"""Command line interfaces."""

# Submodules are available for import but not loaded at package level
# This prevents circular import issues during installation
# Import submodules explicitly when needed:
#   from mgst.cli.main import main
#   from mgst.cli.filter import filter_cmd

__all__ = [
    "database",
    "filter",
    "main",
]