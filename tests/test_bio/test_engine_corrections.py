"""Tests for the rule corrections in the SrvSurvey engine port."""

import os
import sys

import pytest

ENGINE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "experiments", "bio_rule_optimization")
sys.path.insert(0, ENGINE_DIR)

engine = pytest.importorskip("srvsurv_engine")

if not os.path.isdir(engine.SRVSURV_CRITERIA_DIR):
    pytest.skip("SrvSurvey criteria not available", allow_module_level=True)


def props(**overrides):
    base = {
        "PlanetClass": "Rocky body", "SurfaceGravity": 0.1, "SurfaceTemperature": 165.0, "SurfacePressure": 0.02,
        "Atmosphere": "thin carbon dioxide", "AtmosphereType": "CarbonDioxide", "AtmosphereComposition": {"CarbonDioxide": 100.0},
        "DistanceFromArrivalLS": 3000.0, "ChainDistanceLS": 3000.0, "Volcanism": "None", "Materials": None, "Region": "8", "Star": ["F"],
        "StarTemperature": 7000.0, "StarAge": 5000.0, "PrimaryStar": "F", "Nebulae": 5000.0, "Guardian": "False",
    }
    return {**base, **overrides}


def species(preds):
    return {(g, s) for g, s, _ in preds}


def test_tussock_region_added_only_with_corrections():
    assert ("Tussock", "Ignis") in species(engine.predict_props(props()))
    assert ("Tussock", "Ignis") not in species(engine.predict_props(props(), corrections=False))


def test_tussock_other_regions_stay_excluded():
    assert ("Tussock", "Ignis") not in species(engine.predict_props(props(Region="12")))


def test_faithful_criteria_are_not_modified():
    engine.corrected_criteria()
    root = next(r for r in engine.load_criteria() if r["genus"] == "Tussock")
    node = next(n for n in root["children"] if n["species"] == "Ignis")
    regions = next(c for c in node["query"] if c["prop"] == "regions")
    assert not regions["region_ids"] & {8, 16, 17, 35}


@pytest.mark.parametrize("key,p,dropped", [
    (("Frutexa", "Flabellum"), {"AtmosphereType": "Water"}, True),
    (("Frutexa", "Flabellum"), {"AtmosphereType": "Ammonia"}, False),
    (("Electricae", "Radialem"), {"AtmosphereType": "Methane"}, True),
    (("Stratum", "Laminamus"), {"SurfaceTemperature": 165.0}, True),
    (("Stratum", "Laminamus"), {"SurfaceTemperature": 165.1}, False),
    (("Stratum", "Araneamus"), {"StarTemperature": 7500.0}, True),
    (("Stratum", "Araneamus"), {"StarTemperature": 7600.0}, False),
    (("Clypeus", "Speculumi"), {"ChainDistanceLS": 2499.0}, True),
    (("Clypeus", "Speculumi"), {"ChainDistanceLS": 2500.0}, False),
    (("Clypeus", "Speculumi"), {"ChainDistanceLS": 2499.0, "DistanceFromArrivalLS": 90_000.0}, True),
    (("Fonticulua", "Campestris"), {"PlanetClass": "Rocky ice body", "AtmosphereType": "Argon", "StarAge": 10_001.0}, True),
    (("Fonticulua", "Campestris"), {"PlanetClass": "Rocky ice body", "AtmosphereType": "Argon", "StarAge": 10_000.0}, False),
])
def test_vetoes(key, p, dropped):
    assert engine.SPECIES_VETOES[key](props(**p)) is dropped


def test_gravity_veto_uses_spansh_g():
    veto = engine.SPECIES_VETOES[("Tubus", "Rosarium")]
    over = 0.1516 * engine.G_TO_MS2 / 10
    under = 0.1514 * engine.G_TO_MS2 / 10
    assert veto(props(SurfaceGravity=over))
    assert not veto(props(SurfaceGravity=under))
    assert not veto(props(SurfaceGravity=None))


def test_tussock_region_9_removed_only_with_corrections():
    assert ("Tussock", "Ignis") not in species(engine.predict_props(props(Region="9")))
    assert ("Tussock", "Ignis") in species(engine.predict_props(props(Region="9"), corrections=False))


def binary_system():
    """Planet around a K star, with a brighter F partner that SrvSurvey places too close."""
    star = {"type": "Star", "parent_0_type": "Null", "parent_0_id": 0, "semiMajorAxis": 100.0, "orbitalPeriod": 1000.0}
    return engine.SystemContext([
        {"bodyId": 1, "name": "A", "subType": "F (White) Star", "solarRadius": 0.69, "surfaceTemperature": 6500.0,
         "argOfPeriapsis": 0.0, **star},
        {"bodyId": 2, "name": "B", "subType": "K (Yellow-Orange) Star", "solarRadius": 0.75, "surfaceTemperature": 4500.0,
         "argOfPeriapsis": 180.0, **star},
        {"bodyId": 3, "name": "B 1", "type": "Planet", "parent_0_type": "Star", "parent_0_id": 2, "parent_1_type": "Null",
         "parent_1_id": 0, "semiMajorAxis": 90.0, "orbitalPeriod": 50.0},
    ])


def test_star_model_choice():
    system = binary_system()
    body = system.bodies[3]
    srv = engine.body_props(body, system, (0.0, 0.0, 0.0), "srvsurvey")
    orbit = engine.body_props(body, system, (0.0, 0.0, 0.0))
    assert srv["Star"] == ["F"]
    assert orbit["Star"] == ["K"]
    assert orbit["StarTemperature"] == 4500.0
    assert engine.body_props(body, system, (0.0, 0.0, 0.0), "orbit")["Star"] == ["K"]
    with pytest.raises(ValueError):
        engine.body_props(body, system, (0.0, 0.0, 0.0), "other")


def test_empty_variant_is_predicted():
    """SrvSurvey keeps an empty variant ("" is not null), as for Brain Trees."""
    node = {"genus": "G", "species": "S", "variant": None, "useCommonChildren": False, "commonChildren": None, "query": [],
            "children": [{"genus": None, "species": None, "variant": "", "useCommonChildren": False,
                          "commonChildren": None, "query": [], "children": []}]}
    out = set()
    engine._predict(node, {}, None, out)
    assert out == {("G", "S", "")}


@pytest.mark.parametrize("planet, atmosphere, age, dropped", [
    ("High metal content body", "Water", 8_000, True),
    ("High metal content body", "Water", 9_000, False),
    ("High metal content body", "Water", None, False),
    ("Rocky body", "Water", 8_000, False),
])
def test_osseus_discus_star_age(planet, atmosphere, age, dropped):
    veto = engine.SPECIES_VETOES[("Osseus", "Discus")]
    assert veto(props(PlanetClass=planet, AtmosphereType=atmosphere, StarAge=age)) is dropped


@pytest.mark.parametrize("planet, atmosphere, age, dropped", [
    ("Rocky ice body", "Argon", 11_000, True),
    ("Rocky ice body", "Argon", 9_500, False),
    ("Rocky ice body", "Argon", None, False),
    ("Rocky ice body", "Neon", 12_500, False),
])
def test_fonticulua_campestris_star_age(planet, atmosphere, age, dropped):
    veto = engine.SPECIES_VETOES[("Fonticulua", "Campestris")]
    assert veto(props(PlanetClass=planet, AtmosphereType=atmosphere, StarAge=age)) is dropped


def test_temperature_limit_drops_species():
    props = {"SurfaceTemperature": 451.0, "SurfaceGravity": 0.5, "PlanetClass": "Rocky body", "AtmosphereType": "Water"}
    assert engine.outside_limits("Tussock", "Virgam", props)
    assert not engine.outside_limits("Tussock", "Virgam", {**props, "SurfaceTemperature": 449.0})


def test_gravity_limit_uses_spansh_g():
    props = {"SurfaceTemperature": 150.0, "SurfaceGravity": 0.28 * engine.G_TO_MS2 / 10.0, "PlanetClass": "Rocky body", "AtmosphereType": "Ammonia"}
    assert engine.outside_limits("Tussock", "Cultro", props)
    assert not engine.outside_limits("Tussock", "Cultro", {**props, "SurfaceGravity": 0.27 * engine.G_TO_MS2 / 10.0})


def test_limit_scoped_to_body_and_atmosphere():
    props = {"SurfaceTemperature": 280.0, "SurfaceGravity": 0.55 * engine.G_TO_MS2 / 10.0, "PlanetClass": "High metal content body", "AtmosphereType": "CarbonDioxide"}
    assert engine.outside_limits("Bacterium", "Tela", props)
    assert not engine.outside_limits("Bacterium", "Tela", {**props, "AtmosphereType": "Ammonia"})


def test_chain_distance_adds_orbits_up_to_first_star():
    system = engine.SystemContext([
        {"bodyId": 1, "type": "Star", "subType": "M (Red dwarf) Star"},
        {"bodyId": 2, "type": "Planet", "semiMajorAxis": 10.0, "parent_0_type": "Star", "parent_0_id": 1},
        {"bodyId": 3, "type": "Planet", "semiMajorAxis": 0.01, "parent_0_type": "Planet", "parent_0_id": 2,
         "parent_1_type": "Star", "parent_1_id": 1},
    ])
    ls, complete = engine.chain_distance(system, system.bodies[3])
    assert complete and ls == pytest.approx(10.01 * engine.LS_PER_AU)


def test_chain_distance_incomplete_when_an_orbit_is_missing():
    system = engine.SystemContext([
        {"bodyId": 1, "type": "Star", "subType": "M (Red dwarf) Star"},
        {"bodyId": 2, "type": "Planet", "parent_0_type": "Star", "parent_0_id": 1},
        {"bodyId": 3, "type": "Planet", "semiMajorAxis": 0.01, "parent_0_type": "Planet", "parent_0_id": 2,
         "parent_1_type": "Star", "parent_1_id": 1},
    ])
    assert engine.chain_distance(system, system.bodies[3])[1] is False


def test_orbit_star_ignores_stars_outside_the_chain():
    """A far brighter star on another branch never competes with the body's own star."""
    star = {"parent_0_type": "Null", "parent_0_id": 0, "semiMajorAxis": 1.0, "orbitalPeriod": 1000.0}
    system = engine.SystemContext([
        {**star, "bodyId": 1, "type": "Star", "subType": "M (Red dwarf) Star", "solarRadius": 0.1, "surfaceTemperature": 3000.0},
        {"bodyId": 2, "type": "Star", "subType": "O (Blue-White) Star", "solarRadius": 500.0, "surfaceTemperature": 40000.0,
         "semiMajorAxis": 1.0, "orbitalPeriod": 1000.0, "parent_0_type": "Star", "parent_0_id": 7},
        {"bodyId": 3, "type": "Planet", "semiMajorAxis": 1.0, "orbitalPeriod": 100.0, "parent_0_type": "Star", "parent_0_id": 1,
         "parent_1_type": "Null", "parent_1_id": 0},
    ])
    assert engine.orbit_star(system.bodies[3], system, system.bodies[1])["bodyId"] == 1
