"""Score one sector's bodies against the species rules, with each body's dominant star.

Works out which star dominates a body only when a predicted species depends on the answer, since
the orbit-averaged model has to read and sample the whole system.
"""

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Set, Tuple

import pyarrow.compute as pc

from mgst.bio.species_rules import (
    HIGH_VALUE_SPECIES,
    PARENT_COLUMNS,
    parent_chain_ls,
    score_body,
    star_barycentres,
)
from mgst.bio.star_model import SystemStars, dominant_star, star_class

STAR_COLUMNS: List[str] = [
    "system_id64", "bodyId", "type", "spectralClass", "mainStar", "absoluteMagnitude", "solarMasses",
    "solarRadius", "surfaceTemperature", "age", "subType", "semiMajorAxis", *PARENT_COLUMNS,
]
ORBIT_COLUMNS: List[str] = [
    "system_id64", "bodyId", "type", "subType", "solarRadius", "surfaceTemperature", "semiMajorAxis",
    "orbitalEccentricity", "orbitalInclination", "ascendingNode", "argOfPeriapsis", "orbitalPeriod", *PARENT_COLUMNS,
]
OWN_STAR_EXCLUSIONS: Dict[str, Set[str]] = {
    s["name"]: set(s["exclude_own_star_classes"]) for s in HIGH_VALUE_SPECIES if s.get("exclude_own_star_classes")
}


def allowed_by_own_star(hits: List[Tuple[str, int]], own_class: Any) -> List[Tuple[str, int]]:
    """The hits that may appear around a star of this class."""
    return [h for h in hits if own_class not in OWN_STAR_EXCLUSIONS.get(h[0], ())]


def _star_decides(hits: List[Tuple[str, int]], classes: Set[str]) -> bool:
    """True when some predicted species is allowed around only some of the candidate stars."""
    for name, _ in hits:
        excluded = OWN_STAR_EXCLUSIONS.get(name)
        if excluded and classes & excluded and classes - excluded:
            return True
    return False


class SectorStars:
    """Stars and orbits of one sector, for filling in what the species rules need on a body row."""

    def __init__(self, reader: Any):
        self.reader = reader
        self.main_star: Dict[int, dict] = {}
        self.systems: Dict[int, SystemStars] = {}
        self.orbit_by_body: Dict[Tuple[int, int], float] = {}
        self.barycentres: Set[Tuple[int, int]] = set()
        table = reader.read_bodies(columns=STAR_COLUMNS)
        # A few small sectors have no orbit on record at all
        if "semiMajorAxis" in table.column_names:
            self.orbit_by_body = {
                k: v for k, v in zip(
                    zip(table.column("system_id64").to_pylist(), table.column("bodyId").to_pylist()),
                    table.column("semiMajorAxis").to_pylist(),
                ) if v
            }
        rows = table.filter(pc.equal(table.column("type"), "Star")).to_pylist()
        self.barycentres = star_barycentres(rows)
        by_system = defaultdict(list)
        for row in rows:
            sid = row.get("system_id64")
            if sid is None:
                continue
            by_system[sid].append(row)
            if row.get("subType") and (row.get("mainStar") or sid not in self.main_star):
                self.main_star[sid] = row
        self.systems = {sid: SystemStars(stars) for sid, stars in by_system.items()}

    def fill(self, row: dict, coords: Any = None) -> None:
        """Set everything score_body needs from the system, except the dominant star's class."""
        sid = row.get("system_id64")
        main = self.main_star.get(sid)
        row["star_class"] = star_class(main) if main else None
        row["parent_star_class"] = None
        row["parent_chain_ls"] = parent_chain_ls(row, self.orbit_by_body, self.barycentres)
        if main:
            row["star_temperature"] = main.get("surfaceTemperature")
            row["star_abs_magnitude"] = main.get("absoluteMagnitude")
            row["star_solar_masses"] = main.get("solarMasses")
            row["star_solar_radius"] = main.get("solarRadius")
            row["star_age"] = main.get("age")
        if coords:
            row["system_x"], row["system_y"], row["system_z"] = coords

    def score(self, rows: Iterable[dict], coords: Dict[int, Tuple[float, float, float]]) -> List[Tuple[dict, List[Tuple[str, int]]]]:
        """(row, hits) for every row, using the dominant star; hits may be empty.

        Sets parent_star_class on the row where it was worked out. It stays None where no predicted
        species depends on it, or where the body has no star in its chain.
        """
        out: List[Tuple[dict, List[Tuple[str, int]]]] = []
        pending: Dict[int, List[int]] = defaultdict(list)
        for row in rows:
            sid = row.get("system_id64")
            self.fill(row, coords.get(sid))
            hits = score_body(row)  # no own star yet, so nothing is excluded by it
            if hits:
                system = self.systems.get(sid)
                stars = [s for s in system.parent_stars(row) if s.get("subType")] if system else []
                classes = {star_class(s) for s in stars}
                if _star_decides(hits, classes):
                    pending[sid].append(len(out))
                else:
                    row["parent_star_class"] = star_class(stars[0]) if stars else None
                    hits = allowed_by_own_star(hits, row["parent_star_class"])
            out.append((row, hits))
        if pending:
            table = self.reader.read_bodies(columns=ORBIT_COLUMNS, system_ids=set(pending))
            by_system = defaultdict(list)
            for row in table.to_pylist():
                by_system[row["system_id64"]].append(row)
            for sid, indexes in pending.items():
                system = SystemStars(by_system[sid])
                for i in indexes:
                    row, hits = out[i]
                    star = dominant_star(row, system)
                    row["parent_star_class"] = star_class(star) if star else None
                    out[i] = (row, allowed_by_own_star(hits, row["parent_star_class"]))
        return out
