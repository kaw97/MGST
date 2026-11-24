"""Time-averaged stellar flux on a body, integrated over every orbit in the system.

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

import os
import sys
from typing import List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mgst.bio.star_model import OrbitModel, luminosity, orbit_position  # noqa: F401
from srvsurv_engine import SystemContext


def rank_stars(system: SystemContext, body: dict, samples: int = 200_000) -> List[dict]:
    """Every star in the system with SrvSurvey's and the orbit-averaged score, brightest first."""
    model = OrbitModel(system, samples)
    srv_candidates = {int(s["bodyId"]) for s in system.parent_stars(body)}
    rows = []
    for star in system.stars:
        inv = model.mean_inverse_square(body, star)
        rows.append({
            "star": star,
            "srvsurvey_candidate": int(star["bodyId"]) in srv_candidates,
            "srvsurvey_score": system.relative_brightness(body, star) if int(star["bodyId"]) in srv_candidates else None,
            "luminosity": luminosity(star),
            "avg_distance": inv ** -0.5,
            "flux": luminosity(star) * inv,
        })
    return sorted(rows, key=lambda r: -r["flux"])
