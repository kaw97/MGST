"""Elite Dangerous galaxy region lookup from coordinates.

Maps (x, y, z) galactic coordinates to one of 42 named regions using a
run-length-encoded 2D grid. Region data sourced from ExploData
(github.com/Silarn/EDMC-ExploData).

BioScan region groups (from bio_data/regions.py) map named groups to
lists of region IDs for species spawn filtering.
"""

import math
from typing import Dict, List, Optional, Set, Tuple

from .region_map_data import regionmap, regions

_X0 = -49985
_Y0 = -40985
_Z0 = -24105

BIOSCAN_REGION_GROUPS: Dict[str, List[int]] = {
    "sagittarius-carina": [1, 4, 9, 18, 19, 20, 21, 22, 23, 40],
    "scutum-centaurus": [1, 4, 9, 10, 11, 12, 24, 25, 26, 42, 28],
}

_REGION_GROUP_SETS: Dict[str, Set[int]] = {
    k: set(v) for k, v in BIOSCAN_REGION_GROUPS.items()
}


def find_region(x: float, y: float, z: float) -> Optional[Tuple[int, str]]:
    """Return (region_id, region_name) for galactic coordinates, or None."""
    px = int((x - _X0) * 83 / 4096)
    pz = int((z - _Z0) * 83 / 4096)

    if px < 0 or pz < 0 or pz >= len(regionmap):
        return None

    row = regionmap[pz]
    rx = 0
    pv = 0
    for rl, pv in row:
        if px < rx + rl:
            break
        rx += rl
    else:
        pv = 0

    if pv == 0:
        return None
    return (pv, regions[pv])


def in_region_group(x: float, y: float, z: float, group: str) -> bool:
    """Check if coordinates fall within a BioScan region group."""
    allowed = _REGION_GROUP_SETS.get(group)
    if allowed is None:
        return False
    result = find_region(x, y, z)
    if result is None:
        return False
    return result[0] in allowed


def distance_from_sol(x: float, y: float, z: float) -> float:
    """Euclidean distance from Sol (0, 0, 0) in light-years."""
    return math.sqrt(x * x + y * y + z * z)
