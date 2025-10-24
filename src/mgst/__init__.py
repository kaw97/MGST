"""
MGST - Mikunn Galactic Search Tool

A comprehensive toolkit for Elite Dangerous galaxy data analysis,
exobiology research, and exploration route planning.
"""

try:
    from .__version__ import __version__
except ImportError:
    __version__ = "dev"

__author__ = "MGST Team"
__email__ = "mgst@example.com"

# Submodules are available for import but not loaded at package level
# This prevents circular import issues during installation
# Import submodules explicitly when needed:
#   from mgst.core import filtering
#   from mgst.data import loaders
#   from mgst.configs import base

__all__ = [
    "__version__",
]