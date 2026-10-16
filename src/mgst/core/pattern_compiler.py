"""Pattern-to-predicate compiler for Parquet pushdown filtering.

Analyzes a JSON pattern and extracts:
- System predicates: PyArrow filter expressions for systems.parquet
- Body predicates: Per-pattern-body-entry filter expressions for bodies.parquet
- Required columns: Minimum column sets needed from each table
- needs_full_reconstruction flag: True if pattern uses features that
  can't be expressed as Parquet predicates (variable binding, parent
  matching, ring matching, glob wildcards)
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow.compute as pc
import pyarrow.dataset as ds

from mgst.data.parquet_schema import _sanitize_col_name

logger = logging.getLogger(__name__)

# Pattern keys that are documentation, not filter criteria
_SKIP_KEYS = {"comment", "description", "note", "_comment"}


def _is_skip_key(key: str) -> bool:
    """Check if a pattern key should be skipped (documentation field)."""
    return key.startswith(("comment", "description", "note", "_"))


class CompiledPattern:
    """Result of compiling a JSON pattern for Parquet pushdown."""

    def __init__(self) -> None:
        # PyArrow filter expressions for systems.parquet
        self.system_filters: List[ds.Expression] = []

        # Per-body-entry filter expressions for bodies.parquet
        # Each entry is a list of filters that must ALL match for that body entry
        self.body_entry_filters: List[List[ds.Expression]] = []

        # Required columns from each table
        self.system_columns: Set[str] = {"id64"}  # Always need id64
        self.body_columns: Set[str] = {"system_id64"}  # Always need FK

        # If True, candidate systems must be fully reconstructed and
        # run through JSONPatternMatcher for correctness
        self.needs_full_reconstruction: bool = False

        # Reasons why full reconstruction is needed (for debugging)
        self.reconstruction_reasons: List[str] = []

    def combined_system_filter(self) -> Optional[ds.Expression]:
        """Combine all system filters with AND logic."""
        if not self.system_filters:
            return None
        result = self.system_filters[0]
        for f in self.system_filters[1:]:
            result = result & f
        return result

    def combined_body_filter(self, entry_idx: int) -> Optional[ds.Expression]:
        """Combine all filters for a body entry with AND logic."""
        if entry_idx >= len(self.body_entry_filters):
            return None
        filters = self.body_entry_filters[entry_idx]
        if not filters:
            return None
        result = filters[0]
        for f in filters[1:]:
            result = result & f
        return result


class PatternCompiler:
    """Compiles JSON patterns into Parquet-pushdown predicates."""

    def compile(self, pattern: dict) -> CompiledPattern:
        """Compile a JSON pattern into pushdown predicates.

        Args:
            pattern: JSON pattern dict (as loaded from pattern file)

        Returns:
            CompiledPattern with filters, required columns, and
            reconstruction flag
        """
        result = CompiledPattern()

        # Process system-level fields
        for key, value in pattern.items():
            if _is_skip_key(key):
                continue

            if key == "bodies":
                if isinstance(value, list):
                    self._compile_bodies(value, result)
                continue

            self._compile_system_field(key, value, result)

        return result

    def _compile_system_field(
        self, key: str, value: Any, result: CompiledPattern
    ) -> None:
        """Compile a single system-level pattern field."""
        if key == "name":
            if value == "*":
                # Wildcard: matches everything, no filter needed
                return
            if isinstance(value, str) and ("*" in value or "?" in value):
                # Glob pattern — can't push down
                result.needs_full_reconstruction = True
                result.reconstruction_reasons.append(
                    f"system name glob: {value}"
                )
                result.system_columns.add("name")
                return
            # Exact name match
            result.system_filters.append(ds.field("name") == value)
            result.system_columns.add("name")
            return

        if key == "controllingFaction":
            if isinstance(value, dict):
                # controllingFaction is flattened to controllingFaction_{subkey}
                for cf_key, cf_val in value.items():
                    if _is_skip_key(cf_key):
                        continue
                    col = f"controllingFaction_{cf_key}"
                    expr = self._value_to_expression(col, cf_val)
                    if expr is not None:
                        result.system_filters.append(expr)
                        result.system_columns.add(col)
                    else:
                        result.needs_full_reconstruction = True
                        result.reconstruction_reasons.append(
                            f"controllingFaction.{cf_key}: {cf_val}"
                        )
            return

        if key == "coords":
            if isinstance(value, dict):
                for coord_key, coord_val in value.items():
                    if coord_key in ("x", "y", "z"):
                        expr = self._value_to_expression(coord_key, coord_val)
                        if expr is not None:
                            result.system_filters.append(expr)
                            result.system_columns.add(coord_key)
            return

        # Generic system-level field
        expr = self._value_to_expression(key, value)
        if expr is not None:
            result.system_filters.append(expr)
            result.system_columns.add(key)
        else:
            result.needs_full_reconstruction = True
            result.reconstruction_reasons.append(f"system field {key}: {value}")

    def _compile_bodies(
        self, body_patterns: List[dict], result: CompiledPattern
    ) -> None:
        """Compile body pattern entries."""
        for body_pattern in body_patterns:
            entry_filters: List[ds.Expression] = []
            has_complex_features = False

            for key, value in body_pattern.items():
                if _is_skip_key(key):
                    continue

                # Variable binding requires full reconstruction
                if isinstance(value, str) and value.startswith("$"):
                    has_complex_features = True
                    result.needs_full_reconstruction = True
                    result.reconstruction_reasons.append(
                        f"body variable binding: {key}={value}"
                    )
                    result.body_columns.add(key)
                    continue

                # Parent matching requires full reconstruction
                if key == "parents":
                    has_complex_features = True
                    result.needs_full_reconstruction = True
                    result.reconstruction_reasons.append(
                        "body parent matching"
                    )
                    continue

                # Ring matching
                if key == "rings":
                    has_complex_features = True
                    result.needs_full_reconstruction = True
                    result.reconstruction_reasons.append(
                        "body ring matching"
                    )
                    continue

                # Compilable body field
                expr = self._value_to_expression(key, value)
                if expr is not None:
                    entry_filters.append(expr)
                    result.body_columns.add(key)
                else:
                    has_complex_features = True
                    result.needs_full_reconstruction = True
                    result.reconstruction_reasons.append(
                        f"body field {key}: {value}"
                    )
                    result.body_columns.add(key)

            result.body_entry_filters.append(entry_filters)

    def _value_to_expression(
        self, field_name: str, pattern_value: Any
    ) -> Optional[ds.Expression]:
        """Convert a pattern value to a PyArrow dataset Expression.

        Returns None if the value can't be expressed as a pushdown predicate.
        """
        # Wildcard — matches everything
        if pattern_value == "*":
            return None

        # Null match
        if pattern_value is None:
            return ds.field(field_name).is_null()

        # Boolean exact match
        if isinstance(pattern_value, bool):
            return ds.field(field_name) == pattern_value

        # Numeric exact match
        if isinstance(pattern_value, (int, float)):
            return ds.field(field_name) == pattern_value

        # String exact match (no wildcards, no variables)
        if isinstance(pattern_value, str):
            if pattern_value.startswith("$"):
                return None  # Variable — can't push down
            if "*" in pattern_value or "?" in pattern_value:
                return None  # Glob — can't push down
            return ds.field(field_name) == pattern_value

        # Range match
        if isinstance(pattern_value, dict):
            if "min" in pattern_value or "max" in pattern_value:
                exprs = []
                if "min" in pattern_value:
                    exprs.append(
                        ds.field(field_name) >= pattern_value["min"]
                    )
                if "max" in pattern_value:
                    exprs.append(
                        ds.field(field_name) <= pattern_value["max"]
                    )
                if len(exprs) == 1:
                    return exprs[0]
                return exprs[0] & exprs[1]
            # Other dict patterns (nested matching) — can't push down
            return None

        # List/OR match
        if isinstance(pattern_value, list):
            non_null = [v for v in pattern_value if v is not None]
            has_null = None in pattern_value

            if non_null and has_null:
                # Match non-null values OR null
                in_expr = ds.field(field_name).isin(non_null)
                null_expr = ds.field(field_name).is_null()
                return in_expr | null_expr
            elif non_null:
                return ds.field(field_name).isin(non_null)
            elif has_null:
                return ds.field(field_name).is_null()

        return None
