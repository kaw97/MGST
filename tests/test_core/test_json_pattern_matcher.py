"""Tests for JSONPatternMatcher - the core pattern matching engine."""

import pytest
from mgst.core.json_pattern_matcher import JSONPatternMatcher, search_systems


# --- Fixtures ---

@pytest.fixture
def sample_system():
    """A realistic system with bodies, parents, and rings."""
    return {
        "name": "Xela ME-R b1-0",
        "id64": 123456,
        "coords": {"x": 100.0, "y": -50.0, "z": 200.0},
        "population": 0,
        "bodies": [
            {
                "bodyId": 1,
                "type": "Star",
                "subType": "K (Yellow-Orange) Star",
                "name": "Xela ME-R b1-0 A",
                "parents": [],
                "rings": [],
            },
            {
                "bodyId": 5,
                "type": "Planet",
                "subType": "Earth-like world",
                "name": "Xela ME-R b1-0 A 3",
                "surfaceTemperature": 290.0,
                "gravity": 1.1,
                "parents": [{"Star": 1}],
                "rings": [
                    {"name": "Ring A", "type": "Rocky", "mass": 1e10},
                ],
            },
            {
                "bodyId": 7,
                "type": "Planet",
                "subType": "Rocky body",
                "name": "Xela ME-R b1-0 A 3 a",
                "surfaceTemperature": 200.0,
                "gravity": 0.3,
                "parents": [{"Planet": 5}],
                "rings": [],
            },
            {
                "bodyId": 9,
                "type": "Planet",
                "subType": "Water world",
                "name": "Xela ME-R b1-0 B 1",
                "surfaceTemperature": 310.0,
                "gravity": 0.8,
                "parents": [{"Star": 1}],
                "rings": [],
            },
        ],
    }


# --- Wildcard & Glob Tests ---

class TestWildcardMatching:
    def test_wildcard_matches_anything(self):
        matcher = JSONPatternMatcher({"name": "*"})
        assert matcher.matches({"name": "Sol"})
        assert matcher.matches({"name": ""})
        assert matcher.matches({"name": 42})
        assert matcher.matches({"name": None})

    def test_glob_pattern_matching(self):
        matcher = JSONPatternMatcher({"name": "*ME-R*"})
        assert matcher.matches({"name": "Xela ME-R b1"})
        assert not matcher.matches({"name": "Sol"})

    def test_glob_case_insensitive(self):
        matcher = JSONPatternMatcher({"name": "*me-r*"})
        assert matcher.matches({"name": "Xela ME-R b1"})

    def test_glob_question_mark(self):
        matcher = JSONPatternMatcher({"name": "Sol ?"})
        assert matcher.matches({"name": "Sol A"})
        assert not matcher.matches({"name": "Sol AB"})

    def test_glob_pattern_rejects_none(self):
        matcher = JSONPatternMatcher({"name": "*ME-R*"})
        assert not matcher.matches({"name": None})


# --- Null Matching ---

class TestNullMatching:
    def test_null_pattern_matches_none(self):
        matcher = JSONPatternMatcher({"population": None})
        assert matcher.matches({"population": None})

    def test_null_pattern_rejects_zero(self):
        matcher = JSONPatternMatcher({"population": None})
        assert not matcher.matches({"population": 0})

    def test_null_pattern_rejects_empty_string(self):
        matcher = JSONPatternMatcher({"population": None})
        assert not matcher.matches({"population": ""})

    def test_null_pattern_matches_missing_key(self):
        matcher = JSONPatternMatcher({"population": None})
        assert matcher.matches({})


# --- Exact Matching ---

class TestExactMatching:
    def test_exact_string_match(self):
        matcher = JSONPatternMatcher({"name": "Earth-like world"})
        assert matcher.matches({"name": "Earth-like world"})
        assert not matcher.matches({"name": "Water world"})

    def test_exact_numeric_match(self):
        matcher = JSONPatternMatcher({"population": 0})
        assert matcher.matches({"population": 0})
        assert not matcher.matches({"population": 1})


# --- List (OR) Matching ---

class TestListMatching:
    def test_list_match_or_semantics(self):
        matcher = JSONPatternMatcher({"name": ["Earth-like world", "Water world"]})
        assert matcher.matches({"name": "Earth-like world"})
        assert matcher.matches({"name": "Water world"})
        assert not matcher.matches({"name": "Rocky body"})

    def test_list_match_with_none(self):
        matcher = JSONPatternMatcher({"population": [0, None]})
        assert matcher.matches({"population": 0})
        assert matcher.matches({"population": None})
        assert not matcher.matches({"population": 1})


# --- Range Matching ---

class TestRangeMatching:
    def test_range_matching(self):
        matcher = JSONPatternMatcher({"temp": {"min": 200, "max": 350}})
        assert matcher.matches({"temp": 275})
        assert not matcher.matches({"temp": 100})
        assert not matcher.matches({"temp": 400})

    def test_range_min_only(self):
        matcher = JSONPatternMatcher({"temp": {"min": 0}})
        assert matcher.matches({"temp": 5})
        assert not matcher.matches({"temp": -1})

    def test_range_max_only(self):
        matcher = JSONPatternMatcher({"temp": {"max": 100}})
        assert matcher.matches({"temp": 50})
        assert not matcher.matches({"temp": 200})

    def test_range_boundary_values(self):
        matcher = JSONPatternMatcher({"temp": {"min": 200, "max": 350}})
        assert matcher.matches({"temp": 200})  # min is inclusive
        assert matcher.matches({"temp": 350})  # max is inclusive

    def test_range_rejects_none(self):
        matcher = JSONPatternMatcher({"temp": {"min": 0, "max": 100}})
        assert not matcher.matches({"temp": None})


# --- Variable Binding ---

class TestVariableBinding:
    def test_variable_binding(self):
        pattern = {
            "bodies": [
                {"bodyId": "$planet", "subType": "Earth-like world"},
                {"subType": "Rocky body", "parents": [{"Planet": "$planet"}]},
            ]
        }
        system = {
            "bodies": [
                {"bodyId": 5, "subType": "Earth-like world", "parents": [], "rings": []},
                {"bodyId": 7, "subType": "Rocky body", "parents": [{"Planet": 5}], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)

    def test_variable_binding_mismatch(self):
        pattern = {
            "bodies": [
                {"bodyId": "$planet", "subType": "Earth-like world"},
                {"subType": "Rocky body", "parents": [{"Planet": "$planet"}]},
            ]
        }
        system = {
            "bodies": [
                {"bodyId": 5, "subType": "Earth-like world", "parents": [], "rings": []},
                {"bodyId": 7, "subType": "Rocky body", "parents": [{"Planet": 99}], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert not matcher.matches(system)


# --- Dict Matching ---

class TestDictMatching:
    def test_dict_matching(self):
        matcher = JSONPatternMatcher({"coords": {"x": {"min": -100, "max": 100}}})
        assert matcher.matches({"coords": {"x": 50}})
        assert not matcher.matches({"coords": {"x": 200}})

    def test_dict_missing_key_treated_as_none(self):
        matcher = JSONPatternMatcher({"missingField": None})
        assert matcher.matches({"name": "Sol"})


# --- Body Matching ---

class TestBodyMatching:
    def test_body_matching_single(self):
        pattern = {"bodies": [{"subType": "Earth-like world"}]}
        system = {
            "bodies": [
                {"subType": "Earth-like world", "parents": [], "rings": []},
                {"subType": "Rocky body", "parents": [], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)

    def test_body_matching_multiple(self):
        pattern = {
            "bodies": [
                {"subType": "Earth-like world"},
                {"subType": "Rocky body"},
            ]
        }
        system = {
            "bodies": [
                {"subType": "Earth-like world", "parents": [], "rings": []},
                {"subType": "Rocky body", "parents": [], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)

    def test_body_matching_backtracking(self, sample_system):
        """Test that backtracking works when first body assignment fails due to variable constraints."""
        pattern = {
            "bodies": [
                {"bodyId": "$planet", "subType": ["Earth-like world", "Water world"]},
                {"subType": "Rocky body", "parents": [{"Planet": "$planet"}]},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        # Should match: ELW (bodyId=5) + Rocky body (parent Planet=5)
        assert matcher.matches(sample_system)

    def test_body_matching_no_match(self):
        pattern = {"bodies": [{"subType": "Black Hole"}]}
        system = {
            "bodies": [
                {"subType": "Earth-like world", "parents": [], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert not matcher.matches(system)

    def test_body_matching_empty_bodies(self):
        pattern = {"bodies": [{"subType": "Earth-like world"}]}
        matcher = JSONPatternMatcher(pattern)
        assert not matcher.matches({"bodies": []})

    def test_body_matching_empty_pattern(self):
        pattern = {"bodies": []}
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches({"bodies": [{"subType": "Rocky body"}]})


# --- Parent Matching ---

class TestParentMatching:
    def test_parent_matching(self):
        pattern = {"bodies": [{"parents": [{"Planet": "$var"}]}]}
        system = {
            "bodies": [
                {"subType": "Rocky body", "parents": [{"Planet": 5}], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)

    def test_parent_key_mismatch(self):
        pattern = {"bodies": [{"parents": [{"Star": "*"}]}]}
        system = {
            "bodies": [
                {"subType": "Rocky body", "parents": [{"Planet": 5}], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert not matcher.matches(system)


# --- Ring Matching ---

class TestRingMatching:
    def test_ring_matching(self):
        pattern = {"bodies": [{"rings": [{"type": "Rocky"}]}]}
        system = {
            "bodies": [
                {
                    "subType": "Planet",
                    "parents": [],
                    "rings": [{"name": "Ring A", "type": "Rocky", "mass": 1e10}],
                },
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)

    def test_ring_empty_means_no_rings(self):
        pattern = {"bodies": [{"rings": []}]}
        system = {
            "bodies": [
                {"subType": "Planet", "parents": [], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)

        # Should fail when body has rings
        system_with_rings = {
            "bodies": [
                {
                    "subType": "Planet",
                    "parents": [],
                    "rings": [{"type": "Rocky"}],
                },
            ]
        }
        assert not matcher.matches(system_with_rings)

    def test_ring_wildcard_means_has_rings(self):
        pattern = {"bodies": [{"rings": ["*"]}]}
        system_with_rings = {
            "bodies": [
                {
                    "subType": "Planet",
                    "parents": [],
                    "rings": [{"type": "Rocky"}],
                },
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system_with_rings)

        # Should fail when body has no rings
        system_no_rings = {
            "bodies": [
                {"subType": "Planet", "parents": [], "rings": []},
            ]
        }
        assert not matcher.matches(system_no_rings)


# --- Comment Fields ---

class TestCommentFields:
    def test_comment_fields_ignored(self):
        pattern = {
            "comment": "This is a test pattern",
            "description": "Should be ignored",
            "_note": "Internal note",
            "name": "*",
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches({"name": "Sol"})

    def test_body_comment_fields_ignored(self):
        pattern = {
            "bodies": [
                {
                    "comment": "Match ELW",
                    "subType": "Earth-like world",
                }
            ]
        }
        system = {
            "bodies": [
                {"subType": "Earth-like world", "parents": [], "rings": []},
            ]
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(system)


# --- End-to-End Tests ---

class TestEndToEnd:
    def test_full_system_match(self, sample_system):
        """End-to-end test with realistic pattern matching a system."""
        pattern = {
            "description": "Systems with ELW and rocky moon",
            "name": "*",
            "bodies": [
                {
                    "comment": "Match ELW",
                    "bodyId": "$planet",
                    "subType": "Earth-like world",
                },
                {
                    "comment": "Match rocky moon orbiting ELW",
                    "subType": "Rocky body",
                    "parents": [{"Planet": "$planet"}],
                },
            ],
        }
        matcher = JSONPatternMatcher(pattern)
        assert matcher.matches(sample_system)

    def test_full_system_no_match(self, sample_system):
        """System doesn't match a pattern requiring Ammonia world."""
        pattern = {
            "name": "*",
            "bodies": [{"subType": "Ammonia world"}],
        }
        matcher = JSONPatternMatcher(pattern)
        assert not matcher.matches(sample_system)

    def test_matches_resets_variables(self, sample_system):
        """Calling matches() twice doesn't leak state."""
        pattern = {
            "bodies": [
                {"bodyId": "$planet", "subType": "Earth-like world"},
                {"subType": "Rocky body", "parents": [{"Planet": "$planet"}]},
            ]
        }
        matcher = JSONPatternMatcher(pattern)

        # First call binds $planet = 5
        assert matcher.matches(sample_system)

        # Second call should reset and work independently
        other_system = {
            "bodies": [
                {"bodyId": 99, "subType": "Earth-like world", "parents": [], "rings": []},
                {"bodyId": 100, "subType": "Rocky body", "parents": [{"Planet": 99}], "rings": []},
            ]
        }
        assert matcher.matches(other_system)
        # Verify $planet was rebound to 99, not stuck on 5
        assert matcher.variables.get("$planet") == 99

    def test_search_systems_function(self, sample_system):
        """Test the search_systems convenience function."""
        pattern = {"name": "*ME-R*"}
        results = search_systems(pattern, [sample_system, {"name": "Sol"}])
        assert len(results) == 1
        assert results[0]["name"] == "Xela ME-R b1-0"
