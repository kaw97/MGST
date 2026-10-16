#!/usr/bin/env python3
"""Find systems with multiple high-value exobiology bodies.

Scans a Parquet galaxy database and applies BioScan-derived species rulesets
to predict which bodies harbor high-value (>=15M cr) exobiology species.
Systems are ranked by number of qualifying bodies and total predicted value.

Supports:
- Full galaxy, sector-filtered, or corridor-filtered searches
- Validation mode comparing predictions against actual genus signals
- Parallel sector scanning with configurable worker count

Usage:
    # Search specific sectors for multi-body systems
    python scripts/find_high_value_systems.py \\
        --database /data/Databases_v2/galaxy_sectors_parquet \\
        --output /tmp/bio_test/results.jsonl \\
        --sectors Col_285_Sector --min-bodies 1 --workers 8

    # Full galaxy search
    python scripts/find_high_value_systems.py \\
        --database /data/Databases_v2/galaxy_sectors_parquet \\
        --output output/high_value_bio/results.jsonl \\
        --min-bodies 3 --workers 8

    # Validation mode
    python scripts/find_high_value_systems.py \\
        --database /data/Databases_v2/galaxy_sectors_parquet \\
        --output /tmp/bio_validate/results.jsonl \\
        --mode validate --workers 8
"""

import argparse
import json
import logging
import math
import multiprocessing
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from mgst.bio.species_rules import (
    BODY_COLUMNS,
    HIGH_VALUE_SPECIES,
    RELEVANT_BODY_TYPES,
    SYSTEM_COLUMNS,
    score_body,
)
from mgst.data.parquet_reader import ParquetSectorReader

logger = logging.getLogger(__name__)

# Type alias for a scored system result
ScoredSystem = Tuple[
    str,   # system_name
    float, float, float,  # x, y, z
    int,   # system_id64
    List[Tuple[str, List[Tuple[str, int]]]],  # [(body_name, [(species, value)])]
]


def _scan_sector(
    sector_dir: str,
    mode: str,
    date_cutoff: Optional[str],
    min_bodies: int,
    max_system_date: Optional[str] = None,
) -> List[ScoredSystem]:
    """Worker function: scan one sector for high-value bio bodies.

    Args:
        sector_dir: Path to sector directory.
        mode: 'search' or 'validate'.
        date_cutoff: If set, only consider bodies updated after this date.
        min_bodies: Minimum qualifying bodies per system to include.
        max_system_date: If set, only consider systems with date < this value
                         or date is None (i.e. unvisited since before this date).

    Returns:
        List of ScoredSystem tuples for qualifying systems.
    """
    reader = ParquetSectorReader(Path(sector_dir))

    if not reader.has_bodies:
        return []

    # If max_system_date is set, pre-filter systems by date to avoid
    # scoring bodies for systems that have been visited recently
    allowed_sids: Optional[Set[int]] = None
    if max_system_date is not None:
        try:
            sys_table = reader.read_systems(columns=SYSTEM_COLUMNS)
        except Exception:
            return []
        if len(sys_table) == 0:
            return []

        # Keep systems where date is None (never visited) or date < cutoff
        date_col = sys_table.column("date")
        is_null = pc.is_null(date_col)
        is_before = pc.less(date_col, pa.scalar(max_system_date, type=pa.string()))
        mask = pc.or_(is_null, is_before)
        filtered_sys = sys_table.filter(mask)
        del sys_table

        if len(filtered_sys) == 0:
            return []

        allowed_sids = set(filtered_sys.column("id64").to_pylist())
        # Cache system rows for later so we don't re-read
        sys_rows_cache = {row["id64"]: row for row in filtered_sys.to_pylist()}
        del filtered_sys

        if not allowed_sids:
            return []

    # Build star class + star properties lookup
    star_lookup = {}
    star_props = {}
    try:
        star_data = reader.read_bodies(
            columns=[
                "system_id64", "type", "spectralClass", "mainStar",
                "absoluteMagnitude", "solarMasses", "solarRadius",
                "surfaceTemperature",
            ],
        )
        star_rows_f = star_data.filter(
            pc.equal(star_data.column("type"), "Star")
        ).to_pylist()
        del star_data
        for srow in star_rows_f:
            s_id = srow.get("system_id64")
            if s_id is None:
                continue
            s_class = srow.get("spectralClass", "")
            is_main = srow.get("mainStar", False)
            if is_main and s_class:
                star_lookup[s_id] = s_class[0]
                star_props[s_id] = srow
            elif s_id not in star_lookup and s_class:
                star_lookup[s_id] = s_class[0]
                star_props[s_id] = srow
        del star_rows_f
    except Exception:
        pass

    # Read bodies with column pruning and pushdown filter on body type
    try:
        body_filter = pc.field("subType").isin(RELEVANT_BODY_TYPES)
        bodies_table = reader.read_bodies(
            columns=BODY_COLUMNS,
            filters=body_filter,
            system_ids=allowed_sids,
        )
    except Exception:
        # Some sectors may have schema mismatches; skip gracefully
        return []

    if len(bodies_table) == 0:
        return []

    body_rows = bodies_table.to_pylist()
    del bodies_table

    # Build system coordinate lookup for region/distance constraints
    sys_coords: Dict[int, Tuple[float, float, float]] = {}
    if max_system_date is not None and sys_rows_cache:
        for sid_c, srow_c in sys_rows_cache.items():
            sys_coords[sid_c] = (
                srow_c.get("x", 0.0) or 0.0,
                srow_c.get("y", 0.0) or 0.0,
                srow_c.get("z", 0.0) or 0.0,
            )
    else:
        try:
            sys_table_coords = reader.read_systems(columns=["id64", "x", "y", "z"])
            for srow_c in sys_table_coords.to_pylist():
                sys_coords[srow_c["id64"]] = (
                    srow_c.get("x", 0.0) or 0.0,
                    srow_c.get("y", 0.0) or 0.0,
                    srow_c.get("z", 0.0) or 0.0,
                )
            del sys_table_coords
        except Exception:
            pass

    # Score each body
    # system_id64 -> [(body_name, [(species, value)])]
    scored_by_system: Dict[int, List[Tuple[str, List[Tuple[str, int]]]]] = (
        defaultdict(list)
    )

    for row in body_rows:
        # In validate mode, filter by date
        if date_cutoff is not None:
            update_time = row.get("updateTime")
            if update_time is None or update_time < date_cutoff:
                continue

        sid = row.get("system_id64")
        row["star_class"] = star_lookup.get(sid)
        sp = star_props.get(sid)
        if sp:
            row["star_temperature"] = sp.get("surfaceTemperature")
            row["star_abs_magnitude"] = sp.get("absoluteMagnitude")
            row["star_solar_masses"] = sp.get("solarMasses")
            row["star_solar_radius"] = sp.get("solarRadius")
        coords = sys_coords.get(sid)
        if coords:
            row["system_x"], row["system_y"], row["system_z"] = coords
        species_hits = score_body(row)
        if not species_hits:
            continue

        body_name = row.get("name", "")
        scored_by_system[sid].append((body_name, species_hits))

    if not scored_by_system:
        return []

    # Filter to systems meeting min_bodies threshold
    qualifying_sids: Set[int] = set()
    for sid, body_list in scored_by_system.items():
        if len(body_list) >= min_bodies:
            qualifying_sids.add(sid)

    if not qualifying_sids:
        return []

    # Read system info for qualifying systems (use cache if available)
    if max_system_date is not None:
        sys_rows = {
            sid: sys_rows_cache[sid]
            for sid in qualifying_sids
            if sid in sys_rows_cache
        }
    else:
        try:
            sys_table = reader.read_systems(
                columns=SYSTEM_COLUMNS,
            )
        except Exception:
            return []

        if len(sys_table) == 0:
            return []

        # Filter to qualifying system IDs
        mask = pc.is_in(
            sys_table.column("id64"),
            value_set=pa.array(list(qualifying_sids), type=pa.int64()),
        )
        sys_table = sys_table.filter(mask)
        sys_rows = {row["id64"]: row for row in sys_table.to_pylist()}
        del sys_table

    results: List[ScoredSystem] = []
    for sid in qualifying_sids:
        sys_row = sys_rows.get(sid)
        if sys_row is None:
            continue
        results.append((
            sys_row.get("name", ""),
            sys_row.get("x", 0.0) or 0.0,
            sys_row.get("y", 0.0) or 0.0,
            sys_row.get("z", 0.0) or 0.0,
            sid,
            scored_by_system[sid],
        ))

    return results


def _scan_sector_validate(
    sector_dir: str,
    date_cutoff: str,
) -> Dict[str, Any]:
    """Worker function for validation mode: compare predictions to actual genus signals.

    Returns per-species stats for this sector.
    """
    reader = ParquetSectorReader(Path(sector_dir))

    if not reader.has_bodies:
        return {"predictions": {}, "actuals": {}, "bonus": 0}

    # Build star class + star properties lookup
    star_lookup = {}
    star_props = {}
    try:
        star_data = reader.read_bodies(
            columns=[
                "system_id64", "type", "spectralClass", "mainStar",
                "absoluteMagnitude", "solarMasses", "solarRadius",
                "surfaceTemperature",
            ],
        )
        star_rows_f = star_data.filter(
            pc.equal(star_data.column("type"), "Star")
        ).to_pylist()
        del star_data
        for srow in star_rows_f:
            s_id = srow.get("system_id64")
            if s_id is None:
                continue
            s_class = srow.get("spectralClass", "")
            is_main = srow.get("mainStar", False)
            if is_main and s_class:
                star_lookup[s_id] = s_class[0]
                star_props[s_id] = srow
            elif s_id not in star_lookup and s_class:
                star_lookup[s_id] = s_class[0]
                star_props[s_id] = srow
        del star_rows_f
    except Exception:
        pass

    try:
        body_filter = pc.field("subType").isin(RELEVANT_BODY_TYPES)
        bodies_table = reader.read_bodies(
            columns=BODY_COLUMNS,
            filters=body_filter,
        )
    except Exception:
        return {"predictions": {}, "actuals": {}, "bonus": 0}

    if len(bodies_table) == 0:
        return {"predictions": {}, "actuals": {}, "bonus": 0}

    body_rows = bodies_table.to_pylist()
    del bodies_table

    # System coordinates for region/distance constraints
    sys_coords_v: Dict[int, Tuple[float, float, float]] = {}
    try:
        sys_table_v = reader.read_systems(columns=["id64", "x", "y", "z"])
        for srow_v in sys_table_v.to_pylist():
            sys_coords_v[srow_v["id64"]] = (
                srow_v.get("x", 0.0) or 0.0,
                srow_v.get("y", 0.0) or 0.0,
                srow_v.get("z", 0.0) or 0.0,
            )
        del sys_table_v
    except Exception:
        pass

    # Per-species tracking
    # predictions[species] = {"predicted": count, "genus_confirmed": count}
    predictions: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"predicted": 0, "genus_confirmed": 0}
    )
    # actuals[genus_signal] = {"total_with_genus": count, "we_predicted": count}
    actuals: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"total_with_genus": 0, "we_predicted": 0}
    )
    bonus_finds = 0

    # Build set of genus signals we track
    tracked_genuses = {s["genus_signal"] for s in HIGH_VALUE_SPECIES}

    for row in body_rows:
        update_time = row.get("updateTime")
        if update_time is None or update_time < date_cutoff:
            continue

        # Parse actual genuses from signals
        genuses_json = row.get("signals_genuses")
        actual_genuses: Set[str] = set()
        if genuses_json:
            try:
                parsed = json.loads(genuses_json)
                if isinstance(parsed, list):
                    actual_genuses = set(parsed)
            except (json.JSONDecodeError, TypeError):
                pass

        # Score the body
        sid_v = row.get("system_id64")
        row["star_class"] = star_lookup.get(sid_v)
        sp = star_props.get(sid_v)
        if sp:
            row["star_temperature"] = sp.get("surfaceTemperature")
            row["star_abs_magnitude"] = sp.get("absoluteMagnitude")
            row["star_solar_masses"] = sp.get("solarMasses")
            row["star_solar_radius"] = sp.get("solarRadius")
        coords_v = sys_coords_v.get(sid_v)
        if coords_v:
            row["system_x"], row["system_y"], row["system_z"] = coords_v
        species_hits = score_body(row)

        # Track predictions
        predicted_genuses: Set[str] = set()
        for species_name, value in species_hits:
            species_def = next(
                s for s in HIGH_VALUE_SPECIES if s["name"] == species_name
            )
            genus = species_def["genus_signal"]
            predicted_genuses.add(genus)
            predictions[species_name]["predicted"] += 1
            if genus in actual_genuses:
                predictions[species_name]["genus_confirmed"] += 1

        # Track actuals for tracked genuses
        for genus in actual_genuses:
            if genus in tracked_genuses:
                actuals[genus]["total_with_genus"] += 1
                if genus in predicted_genuses:
                    actuals[genus]["we_predicted"] += 1

        # Bonus: genera present but not in our tracked set
        untracked = actual_genuses - tracked_genuses - predicted_genuses
        if untracked:
            bonus_finds += len(untracked)

    return {
        "predictions": {k: dict(v) for k, v in predictions.items()},
        "actuals": {k: dict(v) for k, v in actuals.items()},
        "bonus": bonus_finds,
    }


def discover_sectors(
    database_dir: Path,
    sector_filter: Optional[List[str]] = None,
    corridor: Optional[Dict[str, Any]] = None,
) -> List[Path]:
    """Discover sector directories, optionally filtered.

    Args:
        database_dir: Root database directory.
        sector_filter: If set, only include sectors whose name starts with
                       any of these prefixes.
        corridor: If set, dict with 'start', 'end', 'radius' for spatial
                  filtering using sector index.

    Returns:
        Sorted list of sector directory Paths.
    """
    sectors: List[Path] = []

    for subdir in sorted(database_dir.iterdir()):
        if not subdir.is_dir():
            continue
        if not (subdir / "bodies.parquet").exists():
            continue

        if sector_filter:
            if not any(subdir.name.startswith(prefix) for prefix in sector_filter):
                continue

        sectors.append(subdir)

    if corridor:
        sectors = _filter_sectors_by_corridor(
            sectors, corridor, database_dir
        )

    return sectors


def _filter_sectors_by_corridor(
    sectors: List[Path],
    corridor: Dict[str, Any],
    database_dir: Path,
) -> List[Path]:
    """Filter sectors to those within a corridor using sector index."""
    index_path = database_dir / "sector_index.json"
    if not index_path.exists():
        print(
            f"Warning: No sector_index.json found at {index_path}. "
            "Scanning all sectors (no spatial filtering)."
        )
        return sectors

    with open(index_path) as f:
        sector_index = json.load(f)

    # Handle nested index format: {metadata: ..., sectors: {name: {center_coords: {x,y,z}}}}
    if "sectors" in sector_index and isinstance(sector_index["sectors"], dict):
        sector_entries = sector_index["sectors"]
    else:
        sector_entries = sector_index

    sx, sy, sz = corridor["start"]
    ex, ey, ez = corridor["end"]
    radius = corridor["radius"]

    # Corridor axis vector
    dx, dy, dz = ex - sx, ey - sy, ez - sz
    corridor_len_sq = dx * dx + dy * dy + dz * dz
    if corridor_len_sq == 0:
        # Point search: filter to sectors within radius of start point
        filtered = []
        sector_names = {s.name for s in sectors}
        for sector_name, info in sector_entries.items():
            if sector_name not in sector_names:
                continue
            coords = info.get("center_coords", info)
            cx = coords.get("x", 0)
            cy = coords.get("y", 0)
            cz = coords.get("z", 0)
            dist = math.sqrt((cx - sx)**2 + (cy - sy)**2 + (cz - sz)**2)
            if dist <= radius + 100.0:
                filtered.append(next(s for s in sectors if s.name == sector_name))
        print(f"Sphere filter: {len(filtered)}/{len(sectors)} sectors within {radius} ly")
        return filtered

    corridor_len = math.sqrt(corridor_len_sq)

    # For each sector, check if its center is within (radius + sector_half_diag)
    # of the corridor axis. Sectors are ~cube-shaped, ~40 LY per side typical,
    # so use a generous padding.
    sector_padding = 100.0  # generous half-diagonal estimate
    max_dist = radius + sector_padding

    filtered = []
    sector_names = {s.name for s in sectors}

    for sector_name, info in sector_entries.items():
        if sector_name not in sector_names:
            continue

        coords = info.get("center_coords", info)
        cx = coords.get("x", 0)
        cy = coords.get("y", 0)
        cz = coords.get("z", 0)

        # Vector from corridor start to sector center
        vx, vy, vz = cx - sx, cy - sy, cz - sz
        # Project onto corridor axis
        t = (vx * dx + vy * dy + vz * dz) / corridor_len_sq
        # Clamp t to [0, 1]
        t = max(0.0, min(1.0, t))
        # Closest point on corridor axis
        px, py, pz = sx + t * dx, sy + t * dy, sz + t * dz
        # Distance from sector center to closest point
        dist = math.sqrt(
            (cx - px) ** 2 + (cy - py) ** 2 + (cz - pz) ** 2
        )

        if dist <= max_dist:
            filtered.append(database_dir / sector_name)

    return sorted(filtered)


def run_search(
    database_dir: Path,
    output_path: Path,
    workers: int,
    min_bodies: int,
    sector_filter: Optional[List[str]],
    corridor: Optional[Dict[str, Any]],
    max_system_date: Optional[str] = None,
) -> None:
    """Run the high-value bio search across sectors."""
    sectors = discover_sectors(database_dir, sector_filter, corridor)
    if not sectors:
        print("No sectors found matching filters.")
        return

    print(f"Scanning {len(sectors)} sectors with {workers} workers...")
    print(f"Minimum qualifying bodies per system: {min_bodies}")
    if max_system_date:
        print(f"Filtering to systems with date < {max_system_date} (or no date)")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_results: List[ScoredSystem] = []
    ctx = multiprocessing.get_context("fork")

    try:
        from tqdm import tqdm
        progress = tqdm(total=len(sectors), desc="Sectors", unit="sector")
    except ImportError:
        progress = None

    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=ctx,
    ) as executor:
        futures = {
            executor.submit(
                _scan_sector,
                str(sector),
                "search",
                None,
                min_bodies,
                max_system_date,
            ): sector.name
            for sector in sectors
        }

        for future in as_completed(futures):
            sector_name = futures[future]
            try:
                results = future.result()
                if results:
                    all_results.extend(results)
            except Exception as e:
                print(f"Error processing {sector_name}: {e}")
            if progress:
                progress.update(1)

    if progress:
        progress.close()

    # Sort by body count desc, then total value desc
    all_results.sort(
        key=lambda r: (
            len(r[5]),
            sum(v for _, hits in r[5] for _, v in hits),
        ),
        reverse=True,
    )

    # Write results as TSV
    with open(output_path, "w") as f:
        header = (
            "name\tid64\tspansh_url\tx\ty\tz\t"
            "qualifying_bodies\ttotal_min_value\tbody_details"
        )
        f.write(header + "\n")
        for sys_name, x, y, z, sid, body_hits in all_results:
            spansh_url = f"https://spansh.co.uk/system/{sid}"
            total_val = sum(v for _, hits in body_hits for _, v in hits)
            # Body details: "BodyName(Species1 19M, Species2 16M); BodyName2(...)"
            body_parts = []
            for bname, hits in body_hits:
                species_str = ", ".join(
                    f"{sname} {val / 1_000_000:.1f}M" for sname, val in hits
                )
                body_parts.append(f"{bname}({species_str})")
            body_details = ";".join(body_parts)
            row = (
                f"{sys_name}\t{sid}\t{spansh_url}\t{x:.4f}\t{y:.4f}\t{z:.4f}\t"
                f"{len(body_hits)}\t{total_val}\t{body_details}"
            )
            f.write(row + "\n")

    print(f"\nResults written to {output_path}")
    print(f"Total qualifying systems: {len(all_results)}")

    if all_results:
        total_bodies = sum(len(r[5]) for r in all_results)
        total_value = sum(
            v for r in all_results for _, hits in r[5] for _, v in hits
        )
        max_bodies = max(len(r[5]) for r in all_results)
        print(f"Total qualifying bodies: {total_bodies}")
        print(f"Total predicted min value: {total_value:,.0f} cr")
        print(f"Max bodies in one system: {max_bodies}")

        # Species breakdown
        species_counts: Dict[str, int] = defaultdict(int)
        for r in all_results:
            for _, hits in r[5]:
                for sname, _ in hits:
                    species_counts[sname] += 1

        print("\nSpecies breakdown:")
        for sname, count in sorted(
            species_counts.items(), key=lambda x: -x[1]
        ):
            print(f"  {sname}: {count} bodies")

        # Top 10
        print(f"\nTop 10 systems (by body count):")
        for sys_name, x, y, z, sid, body_hits in all_results[:10]:
            n = len(body_hits)
            val = sum(v for _, hits in body_hits for _, v in hits)
            species_list = set(
                sname for _, hits in body_hits for sname, _ in hits
            )
            print(
                f"  {sys_name} ({x:.1f}, {y:.1f}, {z:.1f}) — "
                f"{n} bodies, {val:,.0f} cr min — "
                f"{', '.join(sorted(species_list))}"
            )


def run_validation(
    database_dir: Path,
    output_path: Path,
    workers: int,
    sector_filter: Optional[List[str]],
) -> None:
    """Run validation comparing predictions to actual genus signals."""
    date_cutoff = "2021-04-01"

    sectors = discover_sectors(database_dir, sector_filter)
    if not sectors:
        print("No sectors found matching filters.")
        return

    print(
        f"Validating predictions on {len(sectors)} sectors "
        f"(bodies updated >= {date_cutoff})..."
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Aggregate stats
    total_predictions: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"predicted": 0, "genus_confirmed": 0}
    )
    total_actuals: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"total_with_genus": 0, "we_predicted": 0}
    )
    total_bonus = 0

    ctx = multiprocessing.get_context("fork")

    try:
        from tqdm import tqdm
        progress = tqdm(total=len(sectors), desc="Validating", unit="sector")
    except ImportError:
        progress = None

    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=ctx,
    ) as executor:
        futures = {
            executor.submit(
                _scan_sector_validate,
                str(sector),
                date_cutoff,
            ): sector.name
            for sector in sectors
        }

        for future in as_completed(futures):
            sector_name = futures[future]
            try:
                stats = future.result()
                for sp_name, sp_stats in stats["predictions"].items():
                    total_predictions[sp_name]["predicted"] += sp_stats[
                        "predicted"
                    ]
                    total_predictions[sp_name][
                        "genus_confirmed"
                    ] += sp_stats["genus_confirmed"]
                for genus, g_stats in stats["actuals"].items():
                    total_actuals[genus][
                        "total_with_genus"
                    ] += g_stats["total_with_genus"]
                    total_actuals[genus]["we_predicted"] += g_stats[
                        "we_predicted"
                    ]
                total_bonus += stats["bonus"]
            except Exception as e:
                print(f"Error processing {sector_name}: {e}")
            if progress:
                progress.update(1)

    if progress:
        progress.close()

    # Print results
    print("\n" + "=" * 80)
    print("PREDICTION VALIDATION RESULTS")
    print(f"Bodies updated since: {date_cutoff}")
    print("=" * 80)

    print(f"\n{'Species':<30} {'Predicted':>10} {'Genus OK':>10} {'Precision':>10}")
    print("-" * 62)
    for sp_name in sorted(total_predictions.keys()):
        stats = total_predictions[sp_name]
        predicted = stats["predicted"]
        confirmed = stats["genus_confirmed"]
        precision = (confirmed / predicted * 100) if predicted > 0 else 0
        print(
            f"{sp_name:<30} {predicted:>10,} {confirmed:>10,} {precision:>9.1f}%"
        )

    # Build genus -> species name mapping for display
    genus_to_species: Dict[str, List[str]] = defaultdict(list)
    for sp in HIGH_VALUE_SPECIES:
        genus_to_species[sp["genus_signal"]].append(sp["name"])

    print(
        f"\n{'Genus Signal':<50} {'Total':>8} {'Predicted':>10} {'Recall':>8}"
    )
    print("-" * 78)
    for genus in sorted(total_actuals.keys()):
        stats = total_actuals[genus]
        total = stats["total_with_genus"]
        predicted = stats["we_predicted"]
        recall = (predicted / total * 100) if total > 0 else 0
        species_names = ", ".join(genus_to_species.get(genus, [genus]))
        print(
            f"{species_names:<50} {total:>8,} {predicted:>10,} {recall:>7.1f}%"
        )

    print(f"\nBonus genus finds (present but not in our 8 targets): {total_bonus:,}")

    # Save validation results as JSON
    validation_output = output_path.parent / "validation_summary.json"
    with open(validation_output, "w") as f:
        json.dump(
            {
                "date_cutoff": date_cutoff,
                "predictions": {
                    k: dict(v) for k, v in total_predictions.items()
                },
                "actuals": {k: dict(v) for k, v in total_actuals.items()},
                "bonus_finds": total_bonus,
            },
            f,
            indent=2,
        )
    print(f"\nValidation summary saved to {validation_output}")


def parse_coords(s: str) -> Tuple[float, float, float]:
    """Parse 'x,y,z' coordinate string."""
    parts = s.split(",")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"Expected 3 comma-separated values, got: {s}"
        )
    return float(parts[0]), float(parts[1]), float(parts[2])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find systems with multiple high-value exobiology bodies.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database",
        type=Path,
        required=True,
        help="Path to Parquet sector database directory.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output JSONL file path.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4).",
    )
    parser.add_argument(
        "--min-bodies",
        type=int,
        default=2,
        help="Minimum qualifying bodies per system (default: 2).",
    )
    parser.add_argument(
        "--mode",
        choices=["search", "validate"],
        default="search",
        help="Mode: 'search' for finding systems, 'validate' for accuracy check.",
    )
    parser.add_argument(
        "--sectors",
        type=str,
        default=None,
        help="Comma-separated sector name prefixes to filter (e.g. 'Col_285_Sector,Lagoon').",
    )
    parser.add_argument(
        "--corridor-start",
        type=str,
        default=None,
        help="Corridor start coordinates 'x,y,z'.",
    )
    parser.add_argument(
        "--corridor-end",
        type=str,
        default=None,
        help="Corridor end coordinates 'x,y,z'.",
    )
    parser.add_argument(
        "--corridor-radius",
        type=float,
        default=None,
        help="Corridor radius in LY.",
    )
    parser.add_argument(
        "--max-system-date",
        type=str,
        default="2021-04-01",
        help=(
            "Only include systems not visited since this date (YYYY-MM-DD) "
            "or never visited. Systems visited after this date are excluded "
            "because their exobiology has likely already been surveyed. "
            "Use --max-system-date none to disable. (default: 2021-04-01)"
        ),
    )

    args = parser.parse_args()

    if args.max_system_date and args.max_system_date.lower() == "none":
        args.max_system_date = None

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    if not args.database.is_dir():
        print(f"Error: Database directory not found: {args.database}")
        sys.exit(1)

    sector_filter = None
    if args.sectors:
        sector_filter = [s.strip() for s in args.sectors.split(",")]

    corridor = None
    if args.corridor_start and args.corridor_end and args.corridor_radius:
        corridor = {
            "start": parse_coords(args.corridor_start),
            "end": parse_coords(args.corridor_end),
            "radius": args.corridor_radius,
        }

    start_time = time.monotonic()

    if args.mode == "validate":
        run_validation(
            args.database,
            args.output,
            args.workers,
            sector_filter,
        )
    else:
        run_search(
            args.database,
            args.output,
            args.workers,
            args.min_bodies,
            sector_filter,
            corridor,
            args.max_system_date,
        )

    elapsed = time.monotonic() - start_time
    print(f"\nCompleted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
