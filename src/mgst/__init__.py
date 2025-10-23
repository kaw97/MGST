"""
HITEC Galaxy Analysis Package

A comprehensive toolkit for Elite Dangerous galaxy data analysis, 
exobiology research, and exploration route planning.
"""

from .__version__ import __version__

__author__ = "HITEC Galaxy Team"
__email__ = "hitec@example.com"

# Main package imports
from .core import clustering, filtering, routing
from .data import converters, loaders
from .configs import base
from .utils import math_utils, file_utils

__all__ = [
    "__version__",
    "clustering", 
    "filtering", 
    "routing",
    "converters",
    "loaders", 
    "base"
]