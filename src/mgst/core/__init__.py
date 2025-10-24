"""Core functionality for galaxy analysis."""

# Submodules are available for import but not loaded at package level
# This prevents circular import issues during installation
# Import submodules explicitly when needed:
#   from mgst.core.filtering import filter_systems
#   from mgst.core.clustering import cluster_systems
#   from mgst.core.routing import calculate_route

__all__ = [
    "clustering",
    "filtering",
    "json_pattern_matcher",
    "pattern_search",
    "routing",
    "search_modes",
    "spatial",
]