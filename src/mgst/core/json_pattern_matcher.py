"""JSON Pattern Matcher - Database-structure mirroring search

This module provides pattern matching for Elite Dangerous system data using JSON patterns
that mirror the exact structure of the database. Patterns support:

- Exact value matching: "subType": "Earth-like world"
- List matching (OR): "subType": ["Earth-like world", "Ammonia world"]
- Range matching: {"min": 0, "max": 100} or {"min": 0} or {"max": 100}
- Wildcard matching: "*" matches anything
- Null matching: null matches missing or null values
- Variable binding: "$varName" creates cross-references between bodies
- Nested structure matching: matches nested dicts and arrays

Pattern structure mirrors database exactly:
{
  "name": "*",
  "coords": {"x": {"min": -1000, "max": 1000}},
  "population": {"max": 1000000},
  "bodies": [
    {
      "bodyId": "$planet",
      "type": "Planet",
      "subType": "Earth-like world"
    },
    {
      "type": "Planet",
      "subType": "Rocky body",
      "parents": [{"Planet": "$planet"}]
    }
  ]
}
"""

from typing import Dict, Any, List, Optional, Set
import re


class JSONPatternMatcher:
    """Matches system data against JSON patterns that mirror database structure."""

    def __init__(self, pattern: Dict[str, Any]):
        """Initialize matcher with a pattern.

        Args:
            pattern: JSON pattern dict mirroring database structure
        """
        self.pattern = pattern
        self.variables: Dict[str, Any] = {}

    def reset(self):
        """Reset variable bindings.

        This is called between systems to ensure variable bindings
        don't leak from one system to another.
        """
        self.variables = {}

    def matches(self, system_data: Dict[str, Any]) -> bool:
        """Check if system matches the pattern.

        Args:
            system_data: System data from database

        Returns:
            True if system matches pattern
        """
        self.variables = {}  # Reset variable bindings
        return self._match_dict(self.pattern, system_data)

    def _match_value(self, pattern_value: Any, data_value: Any) -> bool:
        """Match a single value against a pattern value.

        Args:
            pattern_value: Pattern value (can be literal, dict with min/max, list, *, $var)
            data_value: Actual data value

        Returns:
            True if value matches pattern
        """
        # Wildcard matches anything
        if pattern_value == "*":
            return True

        # Null pattern matches null or missing
        if pattern_value is None:
            return data_value is None

        # Variable binding
        if isinstance(pattern_value, str) and pattern_value.startswith("$"):
            var_name = pattern_value
            if var_name in self.variables:
                # Variable already bound - must match bound value
                return self.variables[var_name] == data_value
            else:
                # Bind variable to this value
                self.variables[var_name] = data_value
                return True

        # Range matching for numeric values
        if isinstance(pattern_value, dict) and ("min" in pattern_value or "max" in pattern_value):
            if data_value is None:
                return False
            min_val = pattern_value.get("min", float("-inf"))
            max_val = pattern_value.get("max", float("inf"))
            return min_val <= data_value <= max_val

        # List matching (OR semantics)
        if isinstance(pattern_value, list):
            return data_value in pattern_value

        # Dict matching (recursive)
        if isinstance(pattern_value, dict):
            if not isinstance(data_value, dict):
                return False
            return self._match_dict(pattern_value, data_value)

        # Exact match
        return pattern_value == data_value

    def _match_dict(self, pattern_dict: Dict[str, Any], data_dict: Dict[str, Any]) -> bool:
        """Match a dictionary against a pattern dictionary.

        Args:
            pattern_dict: Pattern dictionary
            data_dict: Data dictionary

        Returns:
            True if all pattern keys match corresponding data
        """
        for key, pattern_value in pattern_dict.items():
            # Skip documentation/metadata fields (any key starting with comment, description, note, etc.)
            if key.startswith(("comment", "description", "note", "_")):
                continue

            # Special handling for bodies array
            if key == "bodies" and isinstance(pattern_value, list):
                if not self._match_bodies(pattern_value, data_dict.get("bodies", [])):
                    return False
                continue

            # Get data value (missing keys treated as None)
            data_value = data_dict.get(key)

            if not self._match_value(pattern_value, data_value):
                return False

        return True

    def _match_bodies(self, pattern_bodies: List[Dict], data_bodies: List[Dict]) -> bool:
        """Match body patterns against system bodies.

        Each pattern in pattern_bodies must match at least one body in data_bodies.
        Bodies can be matched by multiple patterns, but variable bindings create constraints.

        Args:
            pattern_bodies: List of body patterns to match
            data_bodies: List of actual bodies in system

        Returns:
            True if all body patterns can be satisfied
        """
        if not pattern_bodies:
            return True

        if not data_bodies:
            return False

        # Try to find a valid assignment of bodies to patterns
        return self._find_body_assignment(pattern_bodies, data_bodies, 0, set())

    def _find_body_assignment(self, patterns: List[Dict], bodies: List[Dict],
                             pattern_idx: int, used_bodies: Set[int]) -> bool:
        """Recursively find valid assignment of bodies to patterns with backtracking.

        Args:
            patterns: List of body patterns
            bodies: List of actual bodies
            pattern_idx: Current pattern index being matched
            used_bodies: Set of body indices already assigned

        Returns:
            True if valid assignment exists for remaining patterns
        """
        # All patterns matched successfully
        if pattern_idx >= len(patterns):
            return True

        pattern = patterns[pattern_idx]

        # Try matching this pattern against each unused body
        for body_idx, body in enumerate(bodies):
            if body_idx in used_bodies:
                continue

            # Save current variable state for backtracking
            saved_vars = self.variables.copy()

            # Try to match this pattern against this body
            if self._match_body_pattern(pattern, body):
                # Mark body as used
                used_bodies.add(body_idx)

                # Try to match remaining patterns
                if self._find_body_assignment(patterns, bodies, pattern_idx + 1, used_bodies):
                    return True

                # Backtrack
                used_bodies.remove(body_idx)

            # Restore variable state
            self.variables = saved_vars

        return False

    def _match_body_pattern(self, pattern: Dict[str, Any], body: Dict[str, Any]) -> bool:
        """Match a single body pattern against a body.

        Args:
            pattern: Body pattern dict
            body: Actual body data

        Returns:
            True if body matches pattern
        """
        for key, pattern_value in pattern.items():
            # Skip documentation/metadata fields
            if key.startswith(("comment", "description", "note", "_")):
                continue

            body_value = body.get(key)

            # Special handling for parents array
            if key == "parents" and isinstance(pattern_value, list):
                if not self._match_parents(pattern_value, body.get("parents", [])):
                    return False
                continue

            # Special handling for rings (empty array means no rings)
            if key == "rings" and pattern_value == []:
                if body.get("rings"):
                    return False
                continue

            if not self._match_value(pattern_value, body_value):
                return False

        return True

    def _match_parents(self, pattern_parents: List[Dict], data_parents: List[Dict]) -> bool:
        """Match parent patterns against body parents.

        Args:
            pattern_parents: Pattern parent list
            data_parents: Actual parent list

        Returns:
            True if parents match pattern
        """
        # Each pattern parent must match at least one data parent
        for pattern_parent in pattern_parents:
            found_match = False
            for data_parent in data_parents:
                if self._match_parent_entry(pattern_parent, data_parent):
                    found_match = True
                    break

            if not found_match:
                return False

        return True

    def _match_parent_entry(self, pattern_parent: Dict[str, Any],
                           data_parent: Dict[str, Any]) -> bool:
        """Match a single parent entry.

        Args:
            pattern_parent: Pattern parent dict (e.g., {"Planet": "$var"} or {"Null": 25})
            data_parent: Actual parent dict

        Returns:
            True if parent entry matches
        """
        # Pattern and data must have same keys
        if set(pattern_parent.keys()) != set(data_parent.keys()):
            return False

        # Match each key-value pair
        for key, pattern_value in pattern_parent.items():
            data_value = data_parent.get(key)
            if not self._match_value(pattern_value, data_value):
                return False

        return True


def load_pattern_from_file(pattern_file: str) -> Dict[str, Any]:
    """Load JSON pattern from file.

    Args:
        pattern_file: Path to JSON pattern file

    Returns:
        Pattern dictionary
    """
    import json
    with open(pattern_file, 'r') as f:
        return json.load(f)


def search_systems(pattern: Dict[str, Any], systems: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Search systems using a JSON pattern.

    Args:
        pattern: JSON pattern dict
        systems: List of system data dicts

    Returns:
        List of systems matching the pattern
    """
    matcher = JSONPatternMatcher(pattern)
    return [system for system in systems if matcher.matches(system)]
