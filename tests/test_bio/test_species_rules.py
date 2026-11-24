"""Tests for species rule matching features and the rules derived from landmark analysis."""

import pyarrow as pa
import pytest

from mgst.bio.sector_scoring import SectorStars
import math

import numpy as np

from mgst.bio.star_model import OrbitModel, SystemStars, orbit_position, star_class
from mgst.bio.species_rules import (
    HIGH_VALUE_SPECIES,
    matches_volcanism,
    parent_chain_ls,
    parent_star_class,
    star_barycentres,
    score_body,
)


def names(hits):
    return {name for name, _ in hits}


def species(name):
    return next(sp for sp in HIGH_VALUE_SPECIES if sp["name"] == name)


# Minimal thin-water body for single-ruleset test species without gravity/temperature limits
WATER_BODY = {"atmosphereType": "Thin Water", "gravity": 0.1, "surfaceTemperature": 300.0}

# Thin Water HMC body that matches Cactoida Vermis apart from star class
CACTOIDA_BODY = {
    "system_id64": 1,
    "subType": "High metal content world",
    "atmosphereType": "Thin Water",
    "volcanismType": "No volcanism",
    "gravity": 0.05,
    "surfaceTemperature": 420.0,
    "surfacePressure": 0.06,
    "system_x": 100.0,
    "system_y": 0.0,
    "system_z": 100.0,
}


class TestVolcanismNoneInList:
    def test_none_entry_accepts_no_volcanism(self):
        assert matches_volcanism("No volcanism", ["None", "Metallic Magma"])
        assert matches_volcanism(None, ["None"])

    def test_list_without_none_rejects_no_volcanism(self):
        assert not matches_volcanism("No volcanism", ["Metallic Magma"])

    def test_listed_volcanism_still_matches(self):
        assert matches_volcanism("Minor metallic magma volcanism", ["None", "Metallic Magma"])
        assert not matches_volcanism("Water geysers volcanism", ["None", "Metallic Magma"])


class TestParentStarClass:
    STARS = {(1, 0): "K", (1, 2): "M"}

    def test_direct_star_parent(self):
        row = {"system_id64": 1, "parent_0_type": "Star", "parent_0_id": 2}
        assert parent_star_class(row, self.STARS) == "M"

    def test_moon_walks_up_to_star(self):
        row = {
            "system_id64": 1,
            "parent_0_type": "Planet", "parent_0_id": 5,
            "parent_1_type": "Star", "parent_1_id": 2,
        }
        assert parent_star_class(row, self.STARS) == "M"

    def test_barycentre_only(self):
        row = {"system_id64": 1, "parent_0_type": "Null", "parent_0_id": 9}
        assert parent_star_class(row, self.STARS) is None

    def test_no_parents(self):
        assert parent_star_class({"system_id64": 1}, self.STARS) is None


class TestOwnStarExclusionList:
    SPECIES = [{
        "name": "X", "min_value": 1,
        "exclude_own_star_classes": ["G"],
        "rulesets": [{"atmosphere": ["Water"]}],
    }]

    def test_excludes_own_star(self):
        assert score_body({**WATER_BODY, "star_class": "M", "parent_star_class": "G"}, self.SPECIES) == []

    def test_ignores_main_star(self):
        assert names(score_body({**WATER_BODY, "star_class": "G", "parent_star_class": "M"}, self.SPECIES)) == {"X"}

    def test_unknown_own_star_not_excluded(self):
        assert names(score_body({**WATER_BODY, "star_class": "G", "parent_star_class": None}, self.SPECIES)) == {"X"}


class TestRequiredConstraint:
    def species(self, required):
        rs = {"atmosphere": ["Water"],
              "constraints": [{"field": "distanceToArrival", "min": 100.0, "required": required}]}
        return [{"name": "X", "min_value": 1, "rulesets": [rs]}]

    def test_missing_value_fails_when_required(self):
        assert score_body(WATER_BODY, self.species(True)) == []

    def test_missing_value_passes_when_optional(self):
        assert names(score_body(WATER_BODY, self.species(False))) == {"X"}

    def test_threshold_applies(self):
        assert score_body({**WATER_BODY, "distanceToArrival": 50.0}, self.species(True)) == []


class TestAnyOfComposition:
    SPECIES = [{
        "name": "X", "min_value": 1,
        "rulesets": [{"atmosphere": ["Water"], "atmosphere_component_any": {"atm_Oxygen": 50.0, "atm_Argon": 50.0}}],
    }]

    def test_any_condition_suffices(self):
        assert names(score_body({**WATER_BODY, "atm_Oxygen": 10.0, "atm_Argon": 60.0}, self.SPECIES)) == {"X"}

    def test_no_condition_met(self):
        assert score_body({**WATER_BODY, "atm_Oxygen": 10.0, "atm_Argon": 10.0}, self.SPECIES) == []


class TestStarExclusions:
    def test_cactoida_excluded_around_own_k_or_b_star(self):
        assert "Cactoida Vermis" not in names(score_body({**CACTOIDA_BODY, "star_class": "M", "parent_star_class": "K"}))
        assert "Cactoida Vermis" not in names(score_body({**CACTOIDA_BODY, "star_class": "M", "parent_star_class": "B"}))

    def test_cactoida_allowed_around_own_m_star_in_k_system(self):
        assert "Cactoida Vermis" in names(score_body({**CACTOIDA_BODY, "star_class": "K", "parent_star_class": "M"}))

    def test_aleoida_gravis_excluded_around_own_g_star(self):
        body = {
            "subType": "Rocky body", "atmosphereType": "Thin Carbon dioxide",
            "volcanismType": "No volcanism", "gravity": 0.1, "surfaceTemperature": 193.0,
            "surfacePressure": 0.06, "star_class": "G", "parent_star_class": "M",
        }
        assert "Aleoida Gravis" in names(score_body(body))
        assert "Aleoida Gravis" not in names(score_body({**body, "parent_star_class": "G"}))

    def test_margaritus_uses_own_star(self):
        body = {
            "subType": "High metal content world", "atmosphereType": "Thin Water",
            "volcanismType": "No volcanism", "gravity": 0.1, "surfaceTemperature": 300.0,
        }
        assert "Clypeus Margaritus" in names(score_body({**body, "star_class": "T", "parent_star_class": "M"}))
        assert "Clypeus Margaritus" not in names(score_body({**body, "star_class": "M", "parent_star_class": "T"}))

    def test_no_species_uses_main_star(self):
        assert [sp["name"] for sp in HIGH_VALUE_SPECIES if sp.get("exclude_star_classes")] == []


class TestStratumTectonicasHmcAtmospheres:
    BODY = {
        "subType": "High metal content world",
        "atmosphereType": "Thin Carbon dioxide-rich",
        "volcanismType": "No volcanism",
        "gravity": 0.3,
        "surfaceTemperature": 200.0,
        "surfacePressure": 0.05,
        "parent_star_class": "M",
        "atm_Oxygen": 10.0,
        "atm_Argon": 0.0,
        "atm_Sulphur_dioxide": 1.0,
    }

    def test_carbon_dioxide_rich_predicted(self):
        assert "Stratum Tectonicas" in names(score_body(self.BODY))

    def test_needs_one_composition_threshold(self):
        assert "Stratum Tectonicas" not in names(score_body({**self.BODY, "atm_Sulphur_dioxide": 0.5}))


class TestOsseusDiscus:
    def test_ammonia_rocky_ice(self):
        body = {
            "subType": "Rocky Ice world", "atmosphereType": "Thin Ammonia",
            "volcanismType": "Minor water magma volcanism", "gravity": 0.05,
            "surfaceTemperature": 170.0, "surfacePressure": 0.01, "atm_Ammonia": 100.0,
        }
        assert "Osseus Discus" in names(score_body(body))

    def test_water_floor_lowered_for_rocky_only(self):
        base = {"atmosphereType": "Thin Water", "volcanismType": "No volcanism", "gravity": 0.05,
                "surfaceTemperature": 395.0}
        assert "Osseus Discus" in names(score_body({**base, "subType": "Rocky body"}))
        assert "Osseus Discus" not in names(score_body({**base, "subType": "High metal content world"}))


class TestReceptaRules:
    SO2 = {"atmosphereType": "Thin Sulphur dioxide", "volcanismType": "No volcanism", "gravity": 0.2,
           "surfaceTemperature": 150.0, "surfacePressure": 0.01, "atm_Sulphur_dioxide": 100.0}

    def test_deltahedronix_icy_without_volcanism_rejected(self):
        assert "Recepta Deltahedronix" not in names(score_body({**self.SO2, "subType": "Icy body"}))

    def test_deltahedronix_rocky_without_volcanism_predicted(self):
        assert "Recepta Deltahedronix" in names(score_body({**self.SO2, "subType": "Rocky body"}))

    def test_conditivus_has_no_carbon_dioxide_rule(self):
        assert all("CarbonDioxide" not in rs["atmosphere"] for rs in species("Recepta Conditivus")["rulesets"])

    def test_umbrux_carbon_dioxide_threshold(self):
        co2 = next(rs for rs in species("Recepta Umbrux")["rulesets"] if "CarbonDioxide" in rs["atmosphere"])
        assert co2["atmosphere_component"] == {"atm_Sulphur_dioxide": 1.10}


class TestSpeculumiChainDistance:
    BODY = {
        "subType": "Rocky body", "atmosphereType": "Thin Carbon dioxide",
        "volcanismType": "No volcanism", "gravity": 0.1, "surfaceTemperature": 193.0,
        "surfacePressure": 0.06, "system_x": 10.0, "system_y": 0.0, "system_z": 10.0,
    }

    def test_near_sol_far_from_star_predicted(self):
        assert "Clypeus Speculumi" in names(score_body({**self.BODY, "distanceToArrival": 3000.0}))

    def test_close_to_star_rejected(self):
        assert "Clypeus Speculumi" not in names(score_body({**self.BODY, "distanceToArrival": 1000.0}))

    def test_missing_distance_rejected(self):
        assert "Clypeus Speculumi" not in names(score_body(self.BODY))

    def test_chain_distance_overrides_arrival(self):
        far_from_arrival = {**self.BODY, "distanceToArrival": 300_000.0}
        assert "Clypeus Speculumi" not in names(score_body({**far_from_arrival, "parent_chain_ls": 1000.0}))
        assert "Clypeus Speculumi" in names(score_body({**self.BODY, "distanceToArrival": 1000.0, "parent_chain_ls": 3000.0}))

    def test_chain_adds_orbits_up_to_first_star(self):
        moon = {"system_id64": 1, "semiMajorAxis": 0.01, "parent_0_type": "Planet", "parent_0_id": 5,
                "parent_1_type": "Star", "parent_1_id": 2, "parent_2_type": "Null", "parent_2_id": 0}
        assert parent_chain_ls(moon, {(1, 5): 10.0, (1, 2): 500.0}, set()) == pytest.approx(10.01 * 499.005)

    def test_chain_stops_at_star_barycentre_only(self):
        star = {"system_id64": 1, "subType": "M (Red dwarf) Star", "parent_0_type": "Null", "parent_0_id": 3,
                "parent_1_type": "Star", "parent_1_id": 0}
        centres = star_barycentres([star])
        assert centres == {(1, 3)}
        body = {"system_id64": 1, "semiMajorAxis": 8.0, "parent_0_type": "Null", "parent_0_id": 3}
        assert parent_chain_ls(body, {}, centres) == pytest.approx(8 * 499.005)

    def test_chain_unknown_when_an_orbit_is_missing(self):
        pair = {"system_id64": 1, "semiMajorAxis": 0.1, "parent_0_type": "Null", "parent_0_id": 9,
                "parent_1_type": "Star", "parent_1_id": 2}
        assert parent_chain_ls(pair, {}, set()) is None


class TestStarAgeRules:
    HMC_WATER = {
        "subType": "High metal content world", "atmosphereType": "Thin Water",
        "volcanismType": "No volcanism", "gravity": 0.05, "surfaceTemperature": 420.0,
        "star_class": "M", "parent_star_class": "M",
    }

    def test_osseus_discus_on_hmc_needs_old_star(self):
        assert "Osseus Discus" not in names(score_body({**self.HMC_WATER, "star_age": 5000.0}))
        assert "Osseus Discus" in names(score_body({**self.HMC_WATER, "star_age": 12000.0}))

    def test_osseus_discus_on_rocky_ignores_age(self):
        assert "Osseus Discus" in names(score_body({**self.HMC_WATER, "subType": "Rocky body", "star_age": 1000.0}))

    def test_cactoida_vermis_on_hmc_needs_young_star(self):
        assert "Cactoida Vermis" in names(score_body({**self.HMC_WATER, "star_age": 5000.0}))
        assert "Cactoida Vermis" not in names(score_body({**self.HMC_WATER, "star_age": 12000.0}))

    def test_cactoida_vermis_on_rocky_ignores_age(self):
        assert "Cactoida Vermis" in names(score_body({**self.HMC_WATER, "subType": "Rocky body", "star_age": 12000.0}))

    def test_missing_age_does_not_exclude(self):
        hits = names(score_body(self.HMC_WATER))
        assert {"Osseus Discus", "Cactoida Vermis"} <= hits


class TestDominantStar:
    """Own-star exclusions use the star giving a body the most light, not the first one up its chain."""

    BODY = {
        "system_id64": 1, "bodyId": 9, "type": "Planet", "subType": "High metal content world", "name": "Candidate",
        "atmosphereType": "Thin Ammonia", "gravity": 0.2, "surfaceTemperature": 170.0, "volcanismType": "No volcanism",
        "semiMajorAxis": 10.0, "parent_0_type": "Null", "parent_0_id": 0,
    }

    @staticmethod
    def _star(body_id, sub_type, radius):
        return {"system_id64": 1, "bodyId": body_id, "type": "Star", "subType": sub_type, "solarRadius": radius,
                "surfaceTemperature": 6000.0, "semiMajorAxis": 0.01, "orbitalPeriod": 100.0, "parent_0_type": "Null", "parent_0_id": 0}

    def _hits(self, rows):
        class Reader:
            def read_bodies(self, columns=None, filters=None, system_ids=None):
                return pa.Table.from_pylist([{c: r.get(c) for c in columns} for r in rows])

        return names(SectorStars(Reader()).score([dict(self.BODY)], {1: (0.0, 0.0, 0.0)})[0][1])

    def test_dominant_excluded_star_blocks(self):
        rows = [self._star(1, "K (Yellow-Orange) Star", 0.5), self._star(2, "A (Blue-White) Star", 3.0), self.BODY]
        assert "Stratum Tectonicas" not in self._hits(rows)

    def test_faint_excluded_star_does_not_block(self):
        rows = [self._star(1, "K (Yellow-Orange) Star", 3.0), self._star(2, "A (Blue-White) Star", 0.5), self.BODY]
        assert "Stratum Tectonicas" in self._hits(rows)

    def test_star_codes(self):
        assert star_class({"subType": "T Tauri Star"}) == "TTS"
        assert star_class({"subType": "T (Brown dwarf) Star"}) == "T"
        assert star_class({"subType": "Neutron Star"}) == "N"


class TestKeplerOrbits:
    """Orbit sampling solves Kepler's equation at any eccentricity and weights positions by time."""

    @pytest.mark.parametrize("e", [0.0, 0.5, 0.9, 0.975, 0.99, 0.999])
    def test_solver_converges(self, e):
        m = np.linspace(0.0, 2 * np.pi, 20_001)
        x, y, _ = orbit_position({"semiMajorAxis": 1.0, "orbitalEccentricity": e}, m)
        # recover E from the in-plane position and check Kepler's equation, modulo 2 pi
        big_e = np.arctan2(y / math.sqrt(1 - e * e), x + e)
        residual = (big_e - e * np.sin(big_e) - m + np.pi) % (2 * np.pi) - np.pi
        assert np.max(np.abs(residual)) < 1e-9

    @pytest.mark.parametrize("e", [0.0, 0.5, 0.95])
    def test_mean_inverse_square_of_one_orbit(self, e):
        star = {"bodyId": 1, "type": "Star", "subType": "K (Yellow-Orange) Star", "solarRadius": 1.0, "surfaceTemperature": 5772.0}
        planet = {"bodyId": 2, "type": "Planet", "semiMajorAxis": 2.0, "orbitalEccentricity": e, "orbitalPeriod": 10.0,
                  "parent_0_type": "Star", "parent_0_id": 1}
        model = OrbitModel(SystemStars([star, planet]), 200_000)
        exact = 1 / (4.0 * math.sqrt(1 - e * e))
        assert model.mean_inverse_square(planet, star) == pytest.approx(exact, rel=0.03)

    def test_binary_partners_stay_opposite(self):
        def star(body_id, a, peri):
            return {"bodyId": body_id, "type": "Star", "subType": "K (Yellow-Orange) Star", "solarRadius": 1.0,
                    "surfaceTemperature": 5772.0, "semiMajorAxis": a, "orbitalPeriod": 100.0, "argOfPeriapsis": peri,
                    "parent_0_type": "Null", "parent_0_id": 0}
        a, b = star(1, 5.0, 0.0), star(2, 5.0, 180.0)
        planet = {"bodyId": 3, "type": "Planet", "semiMajorAxis": 1.0, "orbitalPeriod": 1.37,
                  "parent_0_type": "Star", "parent_0_id": 1, "parent_1_type": "Null", "parent_1_id": 0}
        model = OrbitModel(SystemStars([a, b, planet]), 200_000)
        assert model.mean_inverse_square(planet, b) == pytest.approx(1 / (10.0 ** 2 - 1.0), rel=0.01)


class TestTectonicasLimits:
    @pytest.mark.parametrize("atm,gravity,temperature,expected", [
        ("Thin Sulphur dioxide", 0.45, 199.0, True),
        ("Thin Sulphur dioxide", 0.45, 201.0, False),
        ("Thin Oxygen", 0.45, 180.0, True),
        ("Thin Oxygen", 0.45, 210.0, False),
        ("Thin Ammonia", 0.2, 178.0, False),
        ("Thin Water", 0.05, 420.0, True),
    ])
    def test_limits(self, atm, gravity, temperature, expected):
        row = {"subType": "High metal content world", "atmosphereType": atm, "gravity": gravity, "surfaceTemperature": temperature,
               "volcanismType": "No volcanism", "surfacePressure": 0.05, "atm_Sulphur_dioxide": 5.0}
        assert ("Stratum Tectonicas" in [n for n, _ in score_body(row)]) is expected
