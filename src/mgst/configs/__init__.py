"""Configuration system for research scenarios."""

# Submodules are available for import but not loaded at package level
# This prevents circular import issues during installation
# Import submodules explicitly when needed:
#   from mgst.configs.base import BaseConfig
#   from mgst.configs.exobiology import ExobiologyConfig
#   from mgst.configs.json_pattern import JSONPatternConfig

__all__ = [
    "base",
    "binary_body_search",
    "biological_landmarks",
    "config_loader",
    "exobiology",
    "faction_search",
    "high_value_exobiology",
    "improved_exobiology",
    "json_pattern",
    "rule_based_exobiology_10m_selective",
    "stellar_adapted_exobiology",
    "temperature_range_exobiology",
]