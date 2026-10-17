"""High-value exobiology species definitions and matching engine.

Defines target species (>=10M credits) with environmental rulesets
derived from BioScan spawn conditions. Each species has clear discriminators
that separate it from other species in the same genus.

The matching engine takes flat Parquet body row dicts and returns a list of
(species_name, value) tuples for species whose environmental conditions match.
"""

import math
from typing import Any, Dict, List, Optional, Set, Tuple

from mgst.bio.region import find_region, BIOSCAN_REGION_GROUPS

# ---------------------------------------------------------------------------
# Atmosphere name mapping: BioScan shorthand -> (must_contain, must_not_contain)
#
# Parquet atmosphereType values have prefixes like "Thin", "Thick", "Hot",
# "Hot thick", "Hot thin" before the base atmosphere name.
# We use substring matching with exclusion for "rich" variants.
# ---------------------------------------------------------------------------

ATMOSPHERE_MAP: Dict[str, Tuple[str, Optional[str]]] = {
    "Ammonia": ("Ammonia", None),
    "Argon": ("Argon", "Argon-rich"),
    "ArgonRich": ("Argon-rich", None),
    "CarbonDioxide": ("Carbon dioxide", "Carbon dioxide-rich"),
    "CarbonDioxideRich": ("Carbon dioxide-rich", None),
    "Methane": ("Methane", "Methane-rich"),
    "MethaneRich": ("Methane-rich", None),
    "Neon": ("Neon", "Neon-rich"),
    "NeonRich": ("Neon-rich", None),
    "Nitrogen": ("Nitrogen", None),
    "Oxygen": ("Oxygen", None),
    "SulphurDioxide": ("Sulphur dioxide", None),
    "Water": ("Water", "Water-rich"),
    "WaterRich": ("Water-rich", None),
}


# ---------------------------------------------------------------------------
# Body type mapping: BioScan name -> Parquet subType value
# ---------------------------------------------------------------------------

BODY_TYPE_MAP: Dict[str, str] = {
    "High metal content body": "High metal content world",
    "Icy body": "Icy body",
    "Rocky body": "Rocky body",
    "Rocky Ice world": "Rocky Ice world",
    "Rocky ice body": "Rocky Ice world",
}

# Reverse: Parquet subType -> set of BioScan names that map to it
_PARQUET_TO_BIOSCAN_BODY: Dict[str, List[str]] = {}
for _bk, _pv in BODY_TYPE_MAP.items():
    _PARQUET_TO_BIOSCAN_BODY.setdefault(_pv, []).append(_bk)


# ---------------------------------------------------------------------------
# High-value species definitions
# ---------------------------------------------------------------------------

# Clypeus Speculumi is only found far from the arrival star; bodies without a
# recorded distance fail the check
SPECULUMI_ARRIVAL_DISTANCE: Dict[str, Any] = {
    "field": "distanceToArrival", "min": 2500.0, "required": True,
}

HIGH_VALUE_SPECIES: List[Dict[str, Any]] = [
    # --- Tier 1: >= 19M ---
    {
        "name": "Fonticulua Fluctus",
        "value": 20_000_000,
        "min_value": 20_000_000,
        "genus_signal": "$Codex_Ent_Fonticulus_Genus_Name;",
        "rulesets": [
            {
                "body_type": ["Icy body"],
                "atmosphere": ["Oxygen"],
                "min_gravity": 0.23,
                "max_gravity": 0.276,
                "min_temperature": 143.0,
                "max_temperature": 200.0,
                "min_pressure": 0.012,
            },
        ],
    },
    {
        "name": "Fonticulua Segmentatus",
        "value": 19_010_800,
        "min_value": 19_010_800,
        "genus_signal": "$Codex_Ent_Fonticulus_Genus_Name;",
        "rulesets": [
            {
                "body_type": ["Icy body"],
                "atmosphere": ["Neon", "NeonRich"],
                "volcanism": "None",
                "min_gravity": 0.25,
                "max_gravity": 0.276,
                "min_temperature": 50.0,
                "max_temperature": 75.0,
                "max_pressure": 0.006,
            },
        ],
    },
    {
        "name": "Stratum Tectonicas",
        "value": 19_010_800,
        "min_value": 19_010_800,
        "genus_signal": "$Codex_Ent_Stratum_Genus_Name;",
        # Stratum has no G or A colour variant
        "exclude_own_star_classes": ["G", "A"],
        "rulesets": [
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["Ammonia"],
                "min_gravity": 0.045,
                "max_gravity": 0.37,
                "min_temperature": 165.0,
                "max_temperature": 177.0,
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["CarbonDioxide"],
                "min_gravity": 0.045,
                "max_gravity": 0.50,
                "min_temperature": 165.0,
                "max_temperature": 197.0,
                "constraints": [
                    {"field": "atm_Sulphur_dioxide", "min": 0.99},
                ],
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["SulphurDioxide"],
                "min_gravity": 0.265,
                "max_gravity": 0.60,
                "min_temperature": 165.0,
                "max_temperature": 450.0,
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["Water"],
                "min_gravity": 0.045,
                "max_gravity": 0.063,
                "min_temperature": 390.0,
                "max_temperature": 455.0,
            },
            # SrvSurvey bio-criteria branches
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["CarbonDioxideRich"],
                "volcanism": "None",
                "max_gravity": 0.61,
                "min_temperature": 165.0,
                "max_temperature": 246.0,
                "min_pressure": 0.007,
                "atmosphere_component_any": {
                    "atm_Oxygen": 57.0,
                    "atm_Argon": 50.0,
                    "atm_Sulphur_dioxide": 0.9,
                },
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["Oxygen"],
                "volcanism": [
                    "None",
                    "Major Rocky Magma",
                    "Major Silicate Vapour Geysers",
                    "Minor Metallic Magma",
                    "Minor Silicate Vapour Geysers",
                ],
                "max_gravity": 0.52,
                "min_temperature": 165.0,
                "min_pressure": 0.02,
                "atmosphere_component": {"atm_Sulphur_dioxide": 0.8},
            },
        ],
    },
    {
        "name": "Tussock Stigmasis",
        "value": 19_010_800,
        "min_value": 19_010_800,
        "genus_signal": "$Codex_Ent_Tussocks_Genus_Name;",
        # Tussock has no A colour variant
        "exclude_own_star_classes": ["A"],
        "rulesets": [
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["SulphurDioxide"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 132.0,
                "max_temperature": 180.0,
                "max_pressure": 0.01,
            },
        ],
    },
    # --- Tier 2: >= 16M ---
    {
        "name": "Concha Biconcavis",
        "value": 16_777_215,
        "min_value": 16_777_215,
        "genus_signal": "$Codex_Ent_Conchas_Genus_Name;",
        "rulesets": [
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["Nitrogen"],
                "volcanism": "None",
                "min_gravity": 0.053,
                "max_gravity": 0.275,
                "min_temperature": 42.0,
                "max_temperature": 53.0,
                "max_pressure": 0.01,
            },
        ],
    },
    {
        "name": "Cactoida Vermis",
        "value": 16_202_800,
        "min_value": 16_202_800,
        "genus_signal": "$Codex_Ent_Cactoid_Genus_Name;",
        # Cactoida has no K, B or C colour variant
        "exclude_own_star_classes": ["K", "B", "C"],
        "rulesets": [
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["SulphurDioxide"],
                "volcanism": "None",
                "min_gravity": 0.265,
                "max_gravity": 0.276,
                "min_temperature": 160.0,
                "max_temperature": 210.0,
                "max_pressure": 0.005,
            },
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["Water"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.0612,
            },
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["Water"],
                "volcanism": ["water"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
            },
        ],
    },
    {
        "name": "Fumerola Extremus",
        "value": 16_202_800,
        "min_value": 16_202_800,
        "genus_signal": "$Codex_Ent_Fumerolas_Genus_Name;",
        "rulesets": [
            {
                "body_type": ["Rocky body", "Rocky ice body", "High metal content body"],
                "atmosphere": ["Ammonia"],
                "volcanism": ["silicate", "metallic", "rocky"],
                "min_gravity": 0.04,
                "max_gravity": 0.09,
                "min_temperature": 161.0,
                "max_temperature": 177.0,
                "max_pressure": 0.0135,
            },
            {
                "body_type": ["Rocky body", "Rocky ice body", "High metal content body"],
                "atmosphere": ["Argon"],
                "volcanism": ["silicate", "metallic", "rocky"],
                "min_gravity": 0.07,
                "max_gravity": 0.276,
                "min_temperature": 50.0,
                "max_temperature": 121.0,
            },
            {
                "body_type": ["Rocky body", "Rocky ice body", "High metal content body"],
                "atmosphere": ["Methane"],
                "volcanism": ["silicate", "metallic", "rocky"],
                "min_gravity": 0.025,
                "max_gravity": 0.127,
                "min_temperature": 77.0,
                "max_temperature": 109.0,
                "min_pressure": 0.01,
            },
            {
                "body_type": ["Rocky body", "Rocky ice body"],
                "atmosphere": ["SulphurDioxide"],
                "volcanism": ["silicate", "metallic", "rocky"],
                "min_gravity": 0.2656,
                "max_gravity": 0.276,
                "min_temperature": 54.0,
                "max_temperature": 210.0,
            },
        ],
    },
    {
        "name": "Recepta Deltahedronix",
        "value": 16_202_800,
        "min_value": 12_934_900,
        "genus_signal": "$Codex_Ent_Recepta_Genus_Name;",
        "rulesets": [
            # SrvSurvey bio-criteria branches
            {
                "body_type": ["Icy body"],
                "atmosphere": ["SulphurDioxide"],
                "volcanism": [
                    "Carbon Dioxide Geysers",
                    "Major Water Geysers",
                    "Major Water Magma",
                    "Minor Ammonia Magma",
                    "Minor Carbon Dioxide Geysers",
                    "Minor Methane Magma",
                    "Minor Nitrogen Magma",
                    "Minor Water Magma",
                    "Water Geysers",
                    "Water Magma",
                ],
                "min_gravity": 0.188,
                "max_gravity": 0.276,
                "max_temperature": 272.0,
                "atmosphere_component": {"atm_Sulphur_dioxide": 100.0},
            },
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["SulphurDioxide"],
                "volcanism": [
                    "None",
                    "Major Metallic Magma",
                    "Major Silicate Vapour Geysers",
                    "Metallic Magma",
                    "Minor Metallic Magma",
                    "Minor Rocky Magma",
                    "Minor Silicate Vapour Geysers",
                    "Minor Water Magma",
                ],
                "max_gravity": 0.276,
                "min_temperature": 132.0,
                "max_temperature": 207.0,
                "max_pressure": 0.06,
                "atmosphere_component": {"atm_Sulphur_dioxide": 100.0},
            },
            {
                "body_type": ["Rocky Ice world"],
                "atmosphere": ["SulphurDioxide"],
                "volcanism": [
                    "Major Silicate Vapour Geysers",
                    "Major Water Geysers",
                    "Major Water Magma",
                    "Metallic Magma",
                    "Minor Carbon Dioxide Geysers",
                    "Minor Metallic Magma",
                    "Minor Silicate Vapour Geysers",
                    "Minor Water Geysers",
                    "Water Geysers",
                ],
                "min_gravity": 0.22,
                "max_gravity": 0.276,
                "max_temperature": 231.0,
                "atmosphere_component": {"atm_Sulphur_dioxide": 100.0},
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["SulphurDioxide"],
                "volcanism": "None",
                "max_gravity": 0.276,
                "min_temperature": 132.0,
                "max_temperature": 135.0,
                "max_pressure": 0.0014,
                "atmosphere_component": {"atm_Sulphur_dioxide": 100.0},
            },
        ],
    },
    {
        "name": "Stratum Cucumisis",
        "value": 16_202_800,
        "min_value": 16_202_800,
        "genus_signal": "$Codex_Ent_Stratum_Genus_Name;",
        "region": "sagittarius-carina",
        # Stratum has no A, G, B or N colour variant
        "exclude_own_star_classes": ["A", "G", "B", "N"],
        "rulesets": [
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["CarbonDioxide"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.6,
                "min_temperature": 191.0,
                "max_temperature": 371.0,
            },
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["CarbonDioxideRich"],
                "volcanism": "None",
                "min_gravity": 0.44,
                "max_gravity": 0.56,
                "min_temperature": 210.0,
                "max_temperature": 246.0,
                "min_pressure": 0.01,
            },
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["Oxygen"],
                "min_gravity": 0.4,
                "max_gravity": 0.6,
                "min_temperature": 200.0,
                "max_temperature": 250.0,
                "min_pressure": 0.01,
            },
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["SulphurDioxide"],
                "min_gravity": 0.26,
                "max_gravity": 0.55,
                "min_temperature": 191.0,
                "max_temperature": 373.0,
            },
        ],
    },
    {
        "name": "Clypeus Speculumi",
        "value": 16_202_800,
        "min_value": 16_202_800,
        "genus_signal": "$Codex_Ent_Clypeus_Genus_Name;",
        "rulesets": [
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["CarbonDioxide"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 190.0,
                "max_temperature": 197.0,
                "min_pressure": 0.055,
                "constraints": [SPECULUMI_ARRIVAL_DISTANCE],
            },
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["Water"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "constraints": [SPECULUMI_ARRIVAL_DISTANCE],
            },
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["Water"],
                "volcanism": ["water"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "constraints": [SPECULUMI_ARRIVAL_DISTANCE],
            },
        ],
    },
    # --- 10M-15M tier ---
    {
        "name": "Recepta Conditivus",
        "value": 14_313_700,
        "min_value": 14_313_700,
        "genus_signal": "$Codex_Ent_Recepta_Genus_Name;",
        "rulesets": [
            {
                "atmosphere": ["SulphurDioxide"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 132.0,
                "max_temperature": 275.0,
                "atmosphere_component": {"atm_Sulphur_dioxide": 1.05},
            },
        ],
    },
    {
        "name": "Tussock Virgam",
        "value": 14_313_700,
        "min_value": 14_313_700,
        "genus_signal": "$Codex_Ent_Tussocks_Genus_Name;",
        "exclude_star_classes": ["A", "T", "B"],
        "rulesets": [
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["Water"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.065,
            },
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["Water"],
                "volcanism": ["water"],
                "min_gravity": 0.04,
                "max_gravity": 0.065,
            },
        ],
    },
    {
        "name": "Recepta Umbrux",
        "value": 12_934_900,
        "min_value": 12_934_900,
        "genus_signal": "$Codex_Ent_Recepta_Genus_Name;",
        "rulesets": [
            {
                "atmosphere": ["CarbonDioxide"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 151.0,
                "max_temperature": 200.0,
                "atmosphere_component": {"atm_Sulphur_dioxide": 1.10},
            },
            {
                "atmosphere": ["SulphurDioxide"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 132.0,
                "max_temperature": 273.0,
                "atmosphere_component": {"atm_Sulphur_dioxide": 1.05},
            },
        ],
    },
    {
        "name": "Aleoida Gravis",
        "value": 12_934_900,
        "min_value": 12_934_900,
        "genus_signal": "$Codex_Ent_Aleoids_Genus_Name;",
        # Aleoida has no G colour variant
        "exclude_own_star_classes": ["G"],
        "rulesets": [
            {
                "body_type": ["Rocky body", "High metal content body"],
                "atmosphere": ["CarbonDioxide"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 190.0,
                "max_temperature": 197.0,
                "min_pressure": 0.054,
            },
        ],
    },
    {
        "name": "Clypeus Margaritus",
        "value": 11_873_200,
        "min_value": 11_873_200,
        "genus_signal": "$Codex_Ent_Clypeus_Genus_Name;",
        # Clypeus has no T colour variant
        "exclude_own_star_classes": ["T"],
        "rulesets": [
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["CarbonDioxide"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 190.0,
                "max_temperature": 196.0,
                "min_pressure": 0.050,
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["Water"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.276,
            },
        ],
    },
    {
        "name": "Osseus Discus",
        "value": 12_934_900,
        "min_value": 12_934_900,
        "genus_signal": "$Codex_Ent_Osseus_Genus_Name;",
        "rulesets": [
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["Water"],
                "min_gravity": 0.04,
                "max_gravity": 0.063,
                "min_temperature": 390.0,
                "max_temperature": 455.0,
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["Water"],
                "min_gravity": 0.04,
                "max_gravity": 0.063,
                "min_temperature": 400.0,
                "max_temperature": 455.0,
            },
            # SrvSurvey bio-criteria branches
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["Ammonia"],
                "volcanism": [
                    "Major Silicate Vapour Geysers",
                    "Metallic Magma",
                    "Minor Metallic Magma",
                    "Minor Rocky Magma",
                    "Minor Silicate Vapour Geysers",
                    "Minor Water Magma",
                ],
                "max_gravity": 0.058,
                "min_temperature": 169.0,
                "min_pressure": 0.006,
                "atmosphere_component": {"atm_Ammonia": 100.0},
            },
            {
                "body_type": ["Rocky Ice world"],
                "atmosphere": ["Ammonia"],
                "volcanism": [
                    "Major Rocky Magma",
                    "Major Silicate Vapour Geysers",
                    "Major Water Geysers",
                    "Major Water Magma",
                    "Metallic Magma",
                    "Minor Metallic Magma",
                    "Minor Rocky Magma",
                    "Minor Silicate Vapour Geysers",
                    "Minor Water Magma",
                    "Rocky Magma",
                    "Water Magma",
                ],
                "max_gravity": 0.09,
                "min_temperature": 160.7,
                "atmosphere_component": {"atm_Ammonia": 100.0},
            },
            {
                "body_type": ["High metal content body"],
                "atmosphere": ["Ammonia"],
                "volcanism": [
                    "Major Rocky Magma",
                    "Major Silicate Vapour Geysers",
                    "Minor Metallic Magma",
                    "Minor Rocky Magma",
                    "Minor Silicate Vapour Geysers",
                ],
                "min_gravity": 0.046,
                "max_gravity": 0.054,
                "min_temperature": 174.0,
                "min_pressure": 0.009,
                "atmosphere_component": {"atm_Ammonia": 100.0},
            },
            {
                "body_type": ["Rocky Ice world"],
                "atmosphere": ["Argon"],
                "volcanism": [
                    "Major Rocky Magma",
                    "Major Silicate Vapour Geysers",
                    "Major Water Geysers",
                    "Minor Carbon Dioxide Geysers",
                    "Minor Metallic Magma",
                    "Minor Silicate Vapour Geysers",
                    "Minor Water Geysers",
                    "Silicate Vapour Geysers",
                    "Water Geysers",
                ],
                "min_gravity": 0.22,
                "max_gravity": 0.276,
                "max_temperature": 119.0,
                "atmosphere_component": {"atm_Argon": 100.0},
            },
        ],
    },
    {
        "name": "Tubus Cavas",
        "value": 11_873_200,
        "min_value": 11_873_200,
        "genus_signal": "$Codex_Ent_Tubus_Genus_Name;",
        "region": "scutum-centaurus",
        "rulesets": [
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["CarbonDioxide"],
                "volcanism": "None",
                "min_gravity": 0.04,
                "max_gravity": 0.152,
                "min_temperature": 160.0,
                "max_temperature": 197.0,
                "min_pressure": 0.003,
            },
        ],
    },
    {
        "name": "Frutexa Flammasis",
        "value": 10_326_000,
        "min_value": 10_326_000,
        "genus_signal": "$Codex_Ent_Shrubs_Genus_Name;",
        "region": "scutum-centaurus",
        # Frutexa has no K or A colour variant
        "exclude_own_star_classes": ["K", "A"],
        "rulesets": [
            {
                "body_type": ["Rocky body"],
                "atmosphere": ["Ammonia"],
                "min_gravity": 0.04,
                "max_gravity": 0.276,
                "min_temperature": 152.0,
                "max_temperature": 177.0,
                "max_pressure": 0.0135,
            },
        ],
    },
]


# Columns needed from bodies.parquet
BODY_COLUMNS: List[str] = [
    "system_id64",
    "name",
    "subType",
    "atmosphereType",
    "volcanismType",
    "surfaceTemperature",
    "gravity",
    "surfacePressure",
    "atm_Sulphur_dioxide",
    "atm_Ammonia",
    "atm_Argon",
    "atm_Oxygen",
    "signals_genuses",
    "sig_$SAA_SignalType_Biological;",
    "updateTime",
    "distanceToArrival",
]

# Parent chain columns, used to find the star a body orbits
PARENT_COLUMNS: List[str] = [
    f"parent_{i}_{kind}" for i in range(7) for kind in ("type", "id")
]
BODY_COLUMNS += PARENT_COLUMNS


def parent_star_class(
    body_row: dict,
    star_by_body: Dict[Tuple[int, int], str],
) -> Optional[str]:
    """Spectral class letter of the first star up the body's parent chain.

    star_by_body maps (system_id64, bodyId) to a class letter. Returns None
    when the chain contains no star (e.g. a body orbiting only barycentres).
    """
    sid = body_row.get("system_id64")
    for i in range(7):
        ptype = body_row.get(f"parent_{i}_type")
        if ptype is None:
            return None
        if ptype == "Star":
            return star_by_body.get((sid, body_row.get(f"parent_{i}_id")))
    return None

# Columns needed from systems.parquet
SYSTEM_COLUMNS: List[str] = [
    "id64",
    "name",
    "x",
    "y",
    "z",
    "date",
]

# Relevant body types for pushdown filtering (Parquet subType values)
RELEVANT_BODY_TYPES: List[str] = [
    "High metal content world",
    "Icy body",
    "Rocky body",
    "Rocky Ice world",
]


# ---------------------------------------------------------------------------
# Matching functions
# ---------------------------------------------------------------------------


def matches_atmosphere(
    atm_type: Optional[str],
    ruleset_atm_list: List[str],
) -> bool:
    """Check if a Parquet atmosphereType matches any entry in a ruleset atmosphere list.

    Parquet values look like "Thin Carbon dioxide", "Hot thick Sulphur dioxide", etc.
    BioScan shorthand keys map to (must_contain, must_not_contain) pairs for
    substring matching.

    Bio only spawns on bodies with thin atmospheres. Bodies with thick, hot, or
    bare atmosphere types (e.g. "Carbon dioxide", "Thick Carbon dioxide") do not
    support biological signals.

    Returns False if atm_type is None, empty, or not a thin atmosphere.
    """
    if not atm_type or atm_type == "No atmosphere":
        return False

    # Bio requires thin atmospheres — reject thick/bare/hot variants
    if "Thin " not in atm_type:
        return False

    for bioscan_key in ruleset_atm_list:
        mapping = ATMOSPHERE_MAP.get(bioscan_key)
        if mapping is None:
            continue
        must_contain, must_not_contain = mapping
        if must_contain in atm_type:
            if must_not_contain is None or must_not_contain not in atm_type:
                return True

    return False


def matches_volcanism(
    volc_type: Optional[str],
    ruleset_volc: Any,
) -> bool:
    """Check if a Parquet volcanismType matches a ruleset volcanism constraint.

    Constraint types:
    - 'None': body must have no volcanism (null, empty, or "No volcanism")
    - list of strings: volcanismType must contain at least one as a substring
      (case-insensitive); a "None" entry also accepts bodies without volcanism
    - absent/None: no constraint (always matches)
    """
    if ruleset_volc is None:
        # No constraint
        return True

    if ruleset_volc == "Any":
        # Any volcanism including none
        return True

    if ruleset_volc == "None":
        # Must have no volcanism
        return (
            volc_type is None
            or volc_type == ""
            or volc_type == "No volcanism"
            or volc_type == "None"
        )

    if isinstance(ruleset_volc, list):
        if volc_type is None or volc_type == "" or volc_type == "No volcanism":
            return "None" in ruleset_volc
        volc_lower = volc_type.lower()
        return any(
            keyword.lower() in volc_lower
            for keyword in ruleset_volc
            if keyword != "None"
        )

    return False


def matches_body_type(
    sub_type: Optional[str],
    ruleset_body_types: List[str],
) -> bool:
    """Check if a Parquet subType matches any body type in the ruleset.

    BioScan body type names differ from Parquet subType values, so we map
    through BODY_TYPE_MAP.
    """
    if sub_type is None:
        return False

    for bt in ruleset_body_types:
        mapped = BODY_TYPE_MAP.get(bt, bt)
        if sub_type == mapped:
            return True

    return False


def matches_atmosphere_component(
    body_row: dict,
    component_dict: Dict[str, float],
) -> bool:
    """Check atmosphere component thresholds (e.g., atm_Sulphur_dioxide >= 1.05).

    Returns True if all component constraints are satisfied.
    """
    for col_name, min_value in component_dict.items():
        val = body_row.get(col_name)
        if val is None:
            return False
        if val < min_value:
            return False
    return True


def _check_range(
    value: Optional[float],
    min_val: Optional[float],
    max_val: Optional[float],
) -> bool:
    """Check if value falls within [min_val, max_val]. None value = fail."""
    if value is None:
        return False
    if min_val is not None and value < min_val:
        return False
    if max_val is not None and value > max_val:
        return False
    return True


_REGION_GROUP_SETS: Dict[str, Set[int]] = {
    k: set(v) for k, v in BIOSCAN_REGION_GROUPS.items()
}


def score_body(
    body_row: dict,
    species_list: Optional[List[Dict[str, Any]]] = None,
) -> List[Tuple[str, int]]:
    """Score a flat Parquet body row against all high-value species rulesets.

    Args:
        body_row: A dict from a Parquet body row (flat column names).
            Must include system_x, system_y, system_z for region/distance
            constraints (set by the caller from system coordinates).
            exclude_star_classes is checked against star_class (the system's
            main star); exclude_own_star_classes against parent_star_class
            (the star the body orbits).
        species_list: Species definitions to score against; defaults to
            HIGH_VALUE_SPECIES.

    Returns:
        List of (species_name, min_value) for each matching species.
        Empty list if no species match.
    """
    sub_type = body_row.get("subType")
    atm_type = body_row.get("atmosphereType")
    volc_type = body_row.get("volcanismType")
    gravity = body_row.get("gravity")
    temperature = body_row.get("surfaceTemperature")

    star_class = body_row.get("star_class")
    own_star_class = body_row.get("parent_star_class")
    sys_x = body_row.get("system_x")
    sys_y = body_row.get("system_y")
    sys_z = body_row.get("system_z")
    has_coords = sys_x is not None and sys_y is not None and sys_z is not None

    _cached_region_id: Optional[int] = None
    _region_looked_up = False

    def _get_region_id() -> Optional[int]:
        nonlocal _cached_region_id, _region_looked_up
        if _region_looked_up:
            return _cached_region_id
        _region_looked_up = True
        if not has_coords:
            return None
        result = find_region(sys_x, sys_y, sys_z)
        _cached_region_id = result[0] if result else None
        return _cached_region_id

    matches: List[Tuple[str, int]] = []

    for species in species_list if species_list is not None else HIGH_VALUE_SPECIES:
        excluded = species.get("exclude_star_classes")
        if excluded and star_class and star_class in excluded:
            continue
        own_excluded = species.get("exclude_own_star_classes")
        if own_excluded and own_star_class and own_star_class in own_excluded:
            continue

        region_group = species.get("region")
        if region_group:
            if not has_coords:
                continue
            allowed = _REGION_GROUP_SETS.get(region_group)
            if allowed is None:
                continue
            rid = _get_region_id()
            if rid is None or rid not in allowed:
                continue

        min_dist = species.get("min_distance_from_sol")
        if min_dist is not None:
            if not has_coords:
                continue
            dist_sq = sys_x * sys_x + sys_y * sys_y + sys_z * sys_z
            if dist_sq < min_dist * min_dist:
                continue

        matched = False
        for ruleset in species["rulesets"]:
            rs_excluded = ruleset.get("exclude_star_classes")
            if rs_excluded and star_class and star_class in rs_excluded:
                continue

            # Body type check
            body_types = ruleset.get("body_type")
            if body_types is not None:
                if not matches_body_type(sub_type, body_types):
                    continue

            # Atmosphere check
            if not matches_atmosphere(atm_type, ruleset["atmosphere"]):
                continue

            # Volcanism check
            volc_constraint = ruleset.get("volcanism")
            if not matches_volcanism(volc_type, volc_constraint):
                continue

            # Gravity check
            if not _check_range(
                gravity,
                ruleset.get("min_gravity"),
                ruleset.get("max_gravity"),
            ):
                continue

            # Temperature check
            if not _check_range(
                temperature,
                ruleset.get("min_temperature"),
                ruleset.get("max_temperature"),
            ):
                continue

            # Pressure check (if specified)
            if "min_pressure" in ruleset or "max_pressure" in ruleset:
                pressure = body_row.get("surfacePressure")
                if not _check_range(
                    pressure,
                    ruleset.get("min_pressure"),
                    ruleset.get("max_pressure"),
                ):
                    continue

            # Atmosphere component check (Recepta)
            atm_component = ruleset.get("atmosphere_component")
            if atm_component is not None:
                if not matches_atmosphere_component(body_row, atm_component):
                    continue

            atm_component_any = ruleset.get("atmosphere_component_any")
            if atm_component_any is not None:
                if not any(
                    matches_atmosphere_component(body_row, {col: min_value})
                    for col, min_value in atm_component_any.items()
                ):
                    continue

            # Generic constraints (orbital, star, composition filters);
            # a missing value passes unless the constraint is marked required
            constraints = ruleset.get("constraints")
            if constraints:
                constraint_failed = False
                for c in constraints:
                    val = body_row.get(c["field"])
                    if val is None:
                        if c.get("required"):
                            constraint_failed = True
                            break
                        continue
                    if "min" in c and val < c["min"]:
                        constraint_failed = True
                        break
                    if "max" in c and val > c["max"]:
                        constraint_failed = True
                        break
                if constraint_failed:
                    continue

            matched = True
            break

        if matched:
            matches.append((species["name"], species["min_value"]))

    return matches
