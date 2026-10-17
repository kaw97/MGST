"""Tests for species rule matching features and the rules derived from landmark analysis."""

from mgst.bio.species_rules import (
    HIGH_VALUE_SPECIES,
    matches_volcanism,
    parent_star_class,
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

    def test_no_species_uses_main_star_except_tussock_virgam(self):
        assert [sp["name"] for sp in HIGH_VALUE_SPECIES if sp.get("exclude_star_classes")] == ["Tussock Virgam"]


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


class TestSpeculumiArrivalDistance:
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
