"""
Pattern-Based System Search Engine

This module provides powerful pattern-based searching capabilities that allow users
to describe system characteristics using JSON patterns with wildcard support.

Pattern Features:
- Exact value matching
- Wildcard matching with "*"
- Numeric range matching with "range(min,max)"
- String containment with "contains(substring)"
- Enumeration matching with "oneOf(a,b,c)"
- Nested object pattern matching
- Array pattern matching with element constraints

Input Formats:
- JSONL: Line-by-line system patterns
- Pretty JSON: Single structured system pattern
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class PatternMatchResult:
    """Result of pattern matching operation."""
    matches: bool
    score: float  # 0.0 to 1.0, higher = better match
    details: Dict[str, Any]  # Detailed matching information


class PatternMatcher:
    """Advanced pattern matching engine with wildcard support."""

    def __init__(self):
        self.wildcard_handlers = {
            '*': self._match_wildcard,
            'range': self._match_range,
            'contains': self._match_contains,
            'oneOf': self._match_one_of,
            'exists': self._match_exists,
            'count': self._match_count,
            'any': self._match_any,
            'all': self._match_all,
            'or': self._match_or,
            'and': self._match_and
        }

    def _match_wildcard(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match wildcard pattern '*' - always matches."""
        return PatternMatchResult(
            matches=True,
            score=0.5,  # Neutral score for wildcard
            details={'type': 'wildcard', 'pattern': pattern}
        )

    def _match_range(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match numeric range pattern 'range(min,max)'."""
        # Parse range(min,max) pattern
        match = re.match(r'range\(([^,]+),([^)]+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid range pattern'})

        try:
            min_val = float(match.group(1))
            max_val = float(match.group(2))
            num_value = float(value)

            if min_val <= num_value <= max_val:
                # Calculate score based on position within range
                if max_val == min_val:
                    score = 1.0
                else:
                    score = 1.0 - abs(num_value - (min_val + max_val) / 2) / (max_val - min_val)
                    score = max(0.1, min(1.0, score))  # Clamp between 0.1 and 1.0

                return PatternMatchResult(
                    matches=True,
                    score=score,
                    details={'type': 'range', 'value': num_value, 'range': [min_val, max_val]}
                )

        except (ValueError, TypeError):
            pass

        return PatternMatchResult(
            matches=False,
            score=0.0,
            details={'type': 'range', 'error': 'Value not in range or not numeric'}
        )

    def _match_contains(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match string containment pattern 'contains(substring)'."""
        # Parse contains(substring) pattern
        match = re.match(r'contains\(([^)]+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid contains pattern'})

        substring = match.group(1)
        str_value = str(value).lower()
        substring_lower = substring.lower()

        if substring_lower in str_value:
            # Score based on substring length vs total length
            score = min(1.0, len(substring_lower) / len(str_value))
            return PatternMatchResult(
                matches=True,
                score=score,
                details={'type': 'contains', 'substring': substring, 'found_in': str_value}
            )

        return PatternMatchResult(
            matches=False,
            score=0.0,
            details={'type': 'contains', 'substring': substring, 'searched_in': str_value}
        )

    def _match_one_of(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match enumeration pattern 'oneOf(a,b,c)'."""
        # Parse oneOf(a,b,c) pattern
        match = re.match(r'oneOf\(([^)]+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid oneOf pattern'})

        options = [opt.strip() for opt in match.group(1).split(',')]
        str_value = str(value)

        for option in options:
            if str_value == option:
                return PatternMatchResult(
                    matches=True,
                    score=1.0,
                    details={'type': 'oneOf', 'matched_option': option, 'all_options': options}
                )

        return PatternMatchResult(
            matches=False,
            score=0.0,
            details={'type': 'oneOf', 'value': str_value, 'options': options}
        )

    def _match_exists(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match existence pattern 'exists(true/false)'."""
        # Parse exists(true/false) pattern
        match = re.match(r'exists\((true|false)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid exists pattern'})

        should_exist = match.group(1) == 'true'
        does_exist = value is not None

        matches = should_exist == does_exist
        return PatternMatchResult(
            matches=matches,
            score=1.0 if matches else 0.0,
            details={'type': 'exists', 'should_exist': should_exist, 'does_exist': does_exist}
        )

    def _match_count(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match array/list count pattern 'count(n)' or 'count(range(min,max))'."""
        # Parse count pattern
        match = re.match(r'count\(([^)]+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid count pattern'})

        count_pattern = match.group(1)

        try:
            if hasattr(value, '__len__'):
                actual_count = len(value)
            else:
                actual_count = 1 if value is not None else 0

            # Handle count(n) - exact count
            if count_pattern.isdigit():
                expected_count = int(count_pattern)
                matches = actual_count == expected_count
                score = 1.0 if matches else max(0.0, 1.0 - abs(actual_count - expected_count) / max(1, expected_count))

                return PatternMatchResult(
                    matches=matches,
                    score=score,
                    details={'type': 'count', 'expected': expected_count, 'actual': actual_count}
                )

            # Handle count(range(min,max))
            elif count_pattern.startswith('range('):
                range_result = self._match_range(count_pattern, actual_count)
                range_result.details['type'] = 'count_range'
                return range_result

        except (ValueError, TypeError):
            pass

        return PatternMatchResult(
            matches=False,
            score=0.0,
            details={'type': 'count', 'error': 'Invalid count pattern or value'}
        )

    def _match_any(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match 'any' pattern for arrays - at least one element matches sub-pattern."""
        # Parse any(sub_pattern) pattern
        match = re.match(r'any\((.+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid any pattern'})

        sub_pattern = match.group(1)

        if not hasattr(value, '__iter__') or isinstance(value, str):
            return PatternMatchResult(False, 0.0, {'error': 'any() requires iterable value'})

        best_score = 0.0
        any_match = False
        match_details = []

        for item in value:
            result = self.match_pattern(sub_pattern, item)
            if result.matches:
                any_match = True
                best_score = max(best_score, result.score)
            match_details.append(result.details)

        return PatternMatchResult(
            matches=any_match,
            score=best_score,
            details={'type': 'any', 'sub_pattern': sub_pattern, 'item_results': match_details}
        )

    def _match_all(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match 'all' pattern for arrays - all elements match sub-pattern."""
        # Parse all(sub_pattern) pattern
        match = re.match(r'all\((.+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid all pattern'})

        sub_pattern = match.group(1)

        if not hasattr(value, '__iter__') or isinstance(value, str):
            return PatternMatchResult(False, 0.0, {'error': 'all() requires iterable value'})

        total_score = 0.0
        all_match = True
        match_details = []

        for item in value:
            result = self.match_pattern(sub_pattern, item)
            if not result.matches:
                all_match = False
            total_score += result.score
            match_details.append(result.details)

        avg_score = total_score / len(value) if value else 0.0

        return PatternMatchResult(
            matches=all_match,
            score=avg_score if all_match else 0.0,
            details={'type': 'all', 'sub_pattern': sub_pattern, 'item_results': match_details}
        )

    def _match_or(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match 'or' pattern - at least one sub-pattern matches.

        Examples:
        - or(range(10,20),range(50,60)) - value in either range
        - or(contains(Earth),contains(Water)) - value contains either string
        - or(Planet,Moon) - value equals either option
        """
        # Parse or(pattern1,pattern2,...) pattern
        match = re.match(r'or\((.+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid or pattern'})

        # Parse sub-patterns (handling nested parentheses)
        sub_patterns = self._parse_comma_separated_patterns(match.group(1))

        best_score = 0.0
        any_match = False
        match_details = []

        for sub_pattern in sub_patterns:
            result = self.match_pattern(sub_pattern, value)
            match_details.append({
                'pattern': sub_pattern,
                'result': result.details,
                'matches': result.matches,
                'score': result.score
            })

            if result.matches:
                any_match = True
                best_score = max(best_score, result.score)

        return PatternMatchResult(
            matches=any_match,
            score=best_score,
            details={'type': 'or', 'sub_patterns': sub_patterns, 'pattern_results': match_details}
        )

    def _match_and(self, pattern: str, value: Any) -> PatternMatchResult:
        """Match 'and' pattern - all sub-patterns must match.

        Examples:
        - and(range(10,50),contains(Earth)) - numeric range AND string contains
        - and(exists(true),count(range(1,5))) - exists AND count in range
        """
        # Parse and(pattern1,pattern2,...) pattern
        match = re.match(r'and\((.+)\)', pattern)
        if not match:
            return PatternMatchResult(False, 0.0, {'error': 'Invalid and pattern'})

        # Parse sub-patterns (handling nested parentheses)
        sub_patterns = self._parse_comma_separated_patterns(match.group(1))

        total_score = 0.0
        all_match = True
        match_details = []

        for sub_pattern in sub_patterns:
            result = self.match_pattern(sub_pattern, value)
            match_details.append({
                'pattern': sub_pattern,
                'result': result.details,
                'matches': result.matches,
                'score': result.score
            })

            if result.matches:
                total_score += result.score
            else:
                all_match = False

        avg_score = total_score / len(sub_patterns) if sub_patterns else 0.0

        return PatternMatchResult(
            matches=all_match,
            score=avg_score if all_match else 0.0,
            details={'type': 'and', 'sub_patterns': sub_patterns, 'pattern_results': match_details}
        )

    def _parse_comma_separated_patterns(self, patterns_str: str) -> List[str]:
        """Parse comma-separated patterns while handling nested parentheses.

        Examples:
        - "range(10,20),range(50,60)" -> ["range(10,20)", "range(50,60)"]
        - "contains(a,b),Planet" -> ["contains(a,b)", "Planet"]
        """
        patterns = []
        current_pattern = ""
        paren_depth = 0

        for char in patterns_str:
            if char == '(':
                paren_depth += 1
                current_pattern += char
            elif char == ')':
                paren_depth -= 1
                current_pattern += char
            elif char == ',' and paren_depth == 0:
                # Top-level comma - split here
                if current_pattern.strip():
                    patterns.append(current_pattern.strip())
                current_pattern = ""
            else:
                current_pattern += char

        # Add the last pattern
        if current_pattern.strip():
            patterns.append(current_pattern.strip())

        return patterns

    def match_pattern(self, pattern: Any, value: Any) -> PatternMatchResult:
        """Match a pattern against a value.

        Args:
            pattern: Pattern to match (can be exact value, wildcard string, or dict/list)
            value: Value to match against

        Returns:
            PatternMatchResult with match status, score, and details
        """
        # Exact value match
        if pattern == value:
            return PatternMatchResult(
                matches=True,
                score=1.0,
                details={'type': 'exact', 'pattern': pattern, 'value': value}
            )

        # String pattern matching
        if isinstance(pattern, str):
            # Check for wildcard handlers
            for wildcard_type, handler in self.wildcard_handlers.items():
                if pattern == '*' and wildcard_type == '*':
                    return handler(pattern, value)
                elif pattern.startswith(f'{wildcard_type}(') and wildcard_type != '*':
                    return handler(pattern, value)

            # String contains check for non-wildcard patterns
            if isinstance(value, str) and pattern in value:
                return PatternMatchResult(
                    matches=True,
                    score=0.8,
                    details={'type': 'substring', 'pattern': pattern, 'value': value}
                )

        # Dictionary pattern matching
        elif isinstance(pattern, dict) and isinstance(value, dict):
            return self._match_dict_pattern(pattern, value)

        # List pattern matching
        elif isinstance(pattern, list) and isinstance(value, list):
            return self._match_list_pattern(pattern, value)

        # No match
        return PatternMatchResult(
            matches=False,
            score=0.0,
            details={'type': 'no_match', 'pattern': pattern, 'value': value}
        )

    def _match_dict_pattern(self, pattern: Dict, value: Dict) -> PatternMatchResult:
        """Match dictionary pattern against dictionary value."""
        total_score = 0.0
        matched_keys = 0
        total_keys = len(pattern)
        match_details = {}

        for key, sub_pattern in pattern.items():
            if key in value:
                result = self.match_pattern(sub_pattern, value[key])
                match_details[key] = result.details
                if result.matches:
                    matched_keys += 1
                    total_score += result.score
            else:
                match_details[key] = {'error': 'Key not found in value'}

        overall_matches = matched_keys == total_keys
        avg_score = total_score / total_keys if total_keys > 0 else 0.0

        return PatternMatchResult(
            matches=overall_matches,
            score=avg_score if overall_matches else 0.0,
            details={'type': 'dict', 'matched_keys': matched_keys, 'total_keys': total_keys, 'key_results': match_details}
        )

    def _match_list_pattern(self, pattern: List, value: List) -> PatternMatchResult:
        """Match list pattern against list value."""
        if len(pattern) != len(value):
            return PatternMatchResult(
                matches=False,
                score=0.0,
                details={'type': 'list', 'error': f'Length mismatch: pattern {len(pattern)} vs value {len(value)}'}
            )

        total_score = 0.0
        all_match = True
        item_results = []

        for i, (sub_pattern, sub_value) in enumerate(zip(pattern, value)):
            result = self.match_pattern(sub_pattern, sub_value)
            item_results.append(result.details)
            if result.matches:
                total_score += result.score
            else:
                all_match = False

        avg_score = total_score / len(pattern) if pattern else 0.0

        return PatternMatchResult(
            matches=all_match,
            score=avg_score if all_match else 0.0,
            details={'type': 'list', 'item_results': item_results}
        )


class PatternSearchEngine:
    """Pattern-based system search engine."""

    def __init__(self):
        self.matcher = PatternMatcher()

    def load_pattern_file(self, pattern_file: Path) -> List[Dict[str, Any]]:
        """Load pattern(s) from JSON or JSONL file.

        Args:
            pattern_file: Path to pattern file

        Returns:
            List of pattern dictionaries
        """
        patterns = []

        with open(pattern_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        # Try to parse as pretty JSON first
        try:
            pattern = json.loads(content)
            if isinstance(pattern, dict):
                patterns.append(pattern)
            elif isinstance(pattern, list):
                patterns.extend(pattern)
            else:
                raise ValueError("Pattern must be a dictionary or list of dictionaries")

        except json.JSONDecodeError:
            # Try to parse as JSONL
            for line_num, line in enumerate(content.split('\n'), 1):
                line = line.strip()
                if line:
                    try:
                        pattern = json.loads(line)
                        if isinstance(pattern, dict):
                            patterns.append(pattern)
                        else:
                            logger.warning(f"Line {line_num}: Pattern must be a dictionary, skipping")
                    except json.JSONDecodeError as e:
                        logger.warning(f"Line {line_num}: Invalid JSON - {e}")

        if not patterns:
            raise ValueError("No valid patterns found in file")

        logger.info(f"Loaded {len(patterns)} patterns from {pattern_file}")
        return patterns

    def search_system(self, system_data: Dict[str, Any], patterns: List[Dict[str, Any]],
                     min_score: float = 0.5) -> List[PatternMatchResult]:
        """Search system against patterns.

        Args:
            system_data: System data to search
            patterns: List of patterns to match against
            min_score: Minimum match score to consider a match

        Returns:
            List of PatternMatchResult for patterns that match
        """
        results = []

        for i, pattern in enumerate(patterns):
            result = self.matcher.match_pattern(pattern, system_data)
            if result.matches and result.score >= min_score:
                # Add pattern metadata
                result.details['pattern_index'] = i
                result.details['pattern'] = pattern
                results.append(result)

        return results

    def search_systems_iterator(self, systems_iterator, patterns: List[Dict[str, Any]],
                               min_score: float = 0.5):
        """Search multiple systems using an iterator.

        Args:
            systems_iterator: Iterator yielding system dictionaries
            patterns: List of patterns to match against
            min_score: Minimum match score to consider a match

        Yields:
            Tuples of (system_data, matching_results)
        """
        for system_data in systems_iterator:
            results = self.search_system(system_data, patterns, min_score)
            if results:
                yield system_data, results


def create_example_patterns() -> Dict[str, Dict[str, Any]]:
    """Create example patterns for documentation and testing."""
    return {
        "earthlike_worlds": {
            "name": "contains(Earth-like)",
            "bodies": {
                "any(subType)": "Earth-like world"
            }
        },
        "high_value_systems": {
            "bodies": {
                "count(range(5,20))": "*",
                "any(isLandable)": True
            },
            "population": "range(0,1000000)"
        },
        "binary_stars": {
            "bodyCount": "range(2,10)",
            "bodies": {
                "count(range(2,5))": "*",
                "any(type)": "Star"
            }
        },
        "water_worlds": {
            "bodies": {
                "any(subType)": "oneOf(Water world,Ammonia world)",
                "any(terraformingState)": "contains(Terraformable)"
            }
        },
        "neutron_systems": {
            "bodies": {
                "any(subType)": "Neutron Star",
                "any(surfaceTemperature)": "range(1000000,10000000)"
            }
        },
        "habitable_zone_worlds": {
            "bodies": {
                "any(subType)": "or(Earth-like world,Water world,Ammonia world)",
                "any(surfaceTemperature)": "or(range(250,350),range(180,220))",
                "any(earthMasses)": "and(range(0.1,10),exists(true))"
            }
        },
        "extreme_worlds": {
            "bodies": {
                "any(surfaceTemperature)": "or(range(0,100),range(1000,5000))",
                "any(gravity)": "or(range(0,0.1),range(5,50))"
            }
        },
        "exploration_targets": {
            "bodyCount": "range(3,50)",
            "bodies": {
                "any(type)": "or(Planet,Moon)",
                "any(isLandable)": "or(true,false)",
                "count(range(1,10))": "*"
            },
            "population": "or(0,range(0,1000))"
        },
        "stellar_varieties": {
            "bodies": {
                "any(subType)": "or(contains(G),contains(K),contains(F))",
                "any(surfaceTemperature)": "or(range(3000,4000),range(5000,7000))",
                "any(solarMasses)": "and(range(0.5,2.0),exists(true))"
            }
        },
        "complex_systems": {
            "bodyCount": "and(range(5,50),exists(true))",
            "bodies": {
                "any(type)": "or(Star,Planet,Moon)",
                "count(range(5,30))": "*"
            },
            "coords": {
                "x": "or(range(-1000,1000),range(20000,30000))",
                "y": "range(-500,500)",
                "z": "or(range(-1000,0),range(40000,50000))"
            }
        }
    }


if __name__ == "__main__":
    # Example usage and testing
    engine = PatternSearchEngine()

    # Example system data
    example_system = {
        "name": "Example System",
        "bodyCount": 5,
        "bodies": [
            {"type": "Star", "subType": "G (White-Yellow) Star", "mainStar": True},
            {"type": "Planet", "subType": "Earth-like world", "isLandable": False, "terraformingState": "Terraformable"},
            {"type": "Planet", "subType": "High metal content world", "isLandable": True},
        ],
        "population": 0
    }

    # Test patterns
    patterns = [create_example_patterns()["earthlike_worlds"]]

    results = engine.search_system(example_system, patterns)
    print(f"Found {len(results)} matching patterns")
    for result in results:
        print(f"  Score: {result.score:.2f}, Details: {result.details}")