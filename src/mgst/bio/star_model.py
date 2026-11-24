"""Which star dominates a body's light, for rules that depend on the star a body orbits.

The alternative to SrvSurvey's getRelativeBrightness(), which uses
L / (sum of squared semi-major axes up both orbit chains). Here:

- Each body's position is the vector sum of the Kepler orbits up its parent
  chain, built from a, e, i, ascending node and argument of periapsis.
- Orbits are sampled at random mean anomalies (uniform in time). Unrelated
  orbits get independent phases; the two partners of a binary (children of the
  same parent with the same orbital period) share one phase, so they stay on
  opposite sides of their barycentre.
- Flux = L * <1 / |r_body - r_star|^2>, with L = R^2 (T / 5772)^4 in solar units.

Units are whatever the database uses (Spansh: AU, solar radii, K); only ratios
between stars matter.
"""

from __future__ import annotations

import math
import re
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np

ORBIT_SAMPLES = 5_000  # per orbit; the analysis database was built with the same number
_PAREN_RE = re.compile(r"\((.*)\)")
_CLASS_LETTER_RE = re.compile(r"^([OBAFGKMLTY]) \(")

# Spansh star subType -> journal StarType, for types not of the form "X (...) Star"
STAR_TYPE_MAP: Dict[str, str] = {
    "T Tauri Star": "TTS",
    "Herbig Ae/Be Star": "AeBe",
    "Wolf-Rayet Star": "W",
    "Wolf-Rayet N Star": "WN",
    "Wolf-Rayet NC Star": "WNC",
    "Wolf-Rayet C Star": "WC",
    "Wolf-Rayet O Star": "WO",
    "C Star": "C",
    "CN Star": "CN",
    "CJ Star": "CJ",
    "MS-type Star": "MS",
    "S-type Star": "S",
    "Neutron Star": "N",
    "Black Hole": "H",
    "Supermassive Black Hole": "SupermassiveBlackHole",
}

# Journal star type -> name the criteria use for it (deviation from SrvSurvey)
STAR_CLAUSE_ALIASES: Dict[str, str] = {
    "AeBe": "Ae",
}

def journal_star_type(sub_type: Optional[str]) -> Optional[str]:
    """Journal StarType code for a Spansh star subType."""
    if not sub_type:
        return None
    if sub_type in STAR_TYPE_MAP:
        return STAR_TYPE_MAP[sub_type]
    if sub_type.startswith("White Dwarf"):
        m = _PAREN_RE.search(sub_type)
        return m.group(1) if m else "D"
    m = _CLASS_LETTER_RE.match(sub_type)
    if m:
        return m.group(1)
    return sub_type


def flatten_star_type(star_type: Optional[str]) -> Optional[str]:
    """Util.flattenStarType: D*/W*/C* -> first letter, `X_Giant` -> X; then STAR_CLAUSE_ALIASES."""
    if star_type is None:
        return None
    if star_type in STAR_CLAUSE_ALIASES:
        return STAR_CLAUSE_ALIASES[star_type]
    if star_type[0] in ("D", "W", "C"):
        return star_type[0]
    if len(star_type) > 1 and star_type[1] == "_":
        return star_type[0]
    return star_type


def star_class(star: dict) -> Optional[str]:
    """Class of a star as the rules name it, e.g. K, TTS, N, D."""
    return flatten_star_type(journal_star_type(star.get("subType")))


T_SUN = 5772.0


def _num(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return float(v)


def luminosity(star: dict) -> float:
    r, t = _num(star.get("solarRadius")), _num(star.get("surfaceTemperature"))
    if not r or not t:
        return 0.0
    return r * r * (t / T_SUN) ** 4


def orbit_position(body: dict, mean_anomaly: np.ndarray) -> np.ndarray:
    """(3, N) position of a body relative to its parent at the given mean anomalies."""
    a = _num(body.get("semiMajorAxis")) or 0.0
    e = min(_num(body.get("orbitalEccentricity")) or 0.0, 0.999)
    i, node, peri = (
        math.radians(_num(body.get(k)) or 0.0)
        for k in ("orbitalInclination", "ascendingNode", "argOfPeriapsis")
    )
    # Newton from E = M fails to converge for some M once e >= ~0.975; from E = pi it converges for every e < 1
    E = np.full_like(mean_anomaly, math.pi) if e > 0.8 else mean_anomaly.copy()
    for _ in range(30):
        E = E - (E - e * np.sin(E) - mean_anomaly) / (1 - e * np.cos(E))
    x, y = a * (np.cos(E) - e), a * math.sqrt(1 - e * e) * np.sin(E)
    cO, sO, ci, si, cw, sw = math.cos(node), math.sin(node), math.cos(i), math.sin(i), math.cos(peri), math.sin(peri)
    return np.stack([
        (cO * cw - sO * sw * ci) * x + (-cO * sw - sO * cw * ci) * y,
        (sO * cw + cO * sw * ci) * x + (-sO * sw + cO * cw * ci) * y,
        (sw * si) * x + (cw * si) * y,
    ])


class OrbitModel:
    """Monte Carlo positions for every body in one system."""

    def __init__(self, system: SystemStars, samples: int = 200_000, seed: int = 0):
        self.system = system
        self.samples = samples
        self.rng = np.random.default_rng(seed)
        self._phase: Dict[Tuple, np.ndarray] = {}
        self._pos: Dict[int, np.ndarray] = {}
        # Barycentre rows carry no parents; infer them from the chains of bodies below
        self._parent: Dict[int, int] = {}
        for bid, chain in system._chain.items():
            ids = [bid] + [pid for _, pid in chain]
            for child, parent in zip(ids, ids[1:]):
                self._parent.setdefault(child, parent)

    def _parent_id(self, body: dict) -> Optional[int]:
        return self._parent.get(int(body["bodyId"]))

    def _phase_key(self, body: dict) -> Tuple:
        parent = self._parent_id(body)
        period = _num(body.get("orbitalPeriod"))
        if parent is not None and period:
            for other in self.system.bodies.values():
                if other is body or self._parent_id(other) != parent:
                    continue
                op = _num(other.get("orbitalPeriod"))
                if op and abs(op - period) <= 1e-6 * period:
                    return ("pair", parent, round(period, 3))
        return ("solo", int(body["bodyId"]))

    def _mean_anomaly(self, body: dict) -> np.ndarray:
        key = self._phase_key(body)
        if key not in self._phase:
            self._phase[key] = self.rng.uniform(0, 2 * np.pi, self.samples)
        return self._phase[key]

    def _position_of(self, bid: int) -> np.ndarray:
        """(3, N) position relative to the system root; a barycentre with no row adds no orbit of its own."""
        if bid not in self._pos:
            pos = np.zeros((3, self.samples))
            parent = self._parent.get(bid)
            if parent is not None:
                body = self.system.bodies.get(bid)
                if body is not None:
                    pos = pos + orbit_position(body, self._mean_anomaly(body))
                pos = pos + self._position_of(parent)
            self._pos[bid] = pos
        return self._pos[bid]

    def position(self, body: dict) -> np.ndarray:
        """(3, N) position relative to the system root."""
        return self._position_of(int(body["bodyId"]))

    def mean_inverse_square(self, body: dict, star: dict) -> float:
        d = self.position(body) - self.position(star)
        return float(np.mean(1.0 / np.maximum(np.einsum("ij,ij->j", d, d), 1e-30)))

    def flux(self, body: dict, star: dict) -> float:
        return luminosity(star) * self.mean_inverse_square(body, star)


def _parents(body: dict) -> List[Tuple[str, int]]:
    chain = []
    for i in range(10):
        ptype = body.get(f"parent_{i}_type")
        if ptype is None:
            break
        chain.append((ptype, int(body[f"parent_{i}_id"])))
    return chain


class SystemStars:
    """One system's bodies, keyed by bodyId, and which stars can light each one."""

    def __init__(self, bodies: Iterable[dict]):
        self.bodies: Dict[int, dict] = {}
        for b in bodies:
            if b.get("bodyId") is not None:
                self.bodies[int(b["bodyId"])] = b
        self.stars = [b for b in self.bodies.values() if b.get("type") == "Star"]
        self._chain = {bid: _parents(b) for bid, b in self.bodies.items()}

    def _has_parent(self, body: dict, target_id: int) -> bool:
        # direct parent, or only barycentres in between
        for ptype, pid in self._chain[int(body["bodyId"])]:
            if pid == target_id:
                return True
            if ptype != "Null":
                return False
        return False

    def parent_stars(self, body: dict) -> List[dict]:
        """getParentStars(body, onlyFirst: false)."""
        found: Dict[int, dict] = {}
        for ptype, pid in _parents(body):
            if ptype == "Star":
                if pid in self.bodies:
                    found[pid] = self.bodies[pid]
            elif ptype == "Null":
                for s in self.stars:
                    if not self._chain[int(s["bodyId"])]:
                        continue
                    if int(s["bodyId"]) == pid or self._has_parent(s, pid):
                        found[int(s["bodyId"])] = s
        return list(found.values())


def dominant_star(body: dict, system: SystemStars, samples: int = ORBIT_SAMPLES) -> Optional[dict]:
    """The star in the body's chain, or a barycentre on it, giving the most orbit-averaged light.

    The first candidate if none has the data to compare; None if the chain has no star.
    system must hold every body in the system once there is more than one candidate.
    """
    stars = [s for s in system.parent_stars(body) if s.get("subType")]
    if len(stars) < 2:
        return stars[0] if stars else None
    model = OrbitModel(system, samples)
    target = system.bodies[int(body["bodyId"])]
    flux = [model.flux(target, s) for s in stars]
    return stars[int(np.argmax(flux))] if sum(flux) > 0 else stars[0]
