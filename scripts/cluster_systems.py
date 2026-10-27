#!/usr/bin/env python3
"""
Cluster systems based on proximity.

Identifies groups of systems where every system is within the specified
distance of at least one other system in the cluster.
"""

import argparse
import csv
import difflib
import math
import re
from pathlib import Path
from typing import List, Dict, Set, Tuple
import sys

import numpy as np
from scipy.spatial import KDTree
from sklearn.cluster import DBSCAN
import hdbscan

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from mgst.bio.species_rules import HIGH_VALUE_SPECIES


class UnionFind:
    """Disjoint set data structure for efficient clustering."""

    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        """Find the root of the set containing x with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int) -> bool:
        """Unite the sets containing x and y. Returns True if they were merged."""
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x == root_y:
            return False

        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1

        return True


def calculate_distance(pos1: Tuple[float, float, float],
                       pos2: Tuple[float, float, float]) -> float:
    """Calculate Euclidean distance between two 3D positions."""
    dx = pos1[0] - pos2[0]
    dy = pos1[1] - pos2[1]
    dz = pos1[2] - pos2[2]
    return math.sqrt(dx*dx + dy*dy + dz*dz)


def calculate_corridor_distance(point: Tuple[float, float, float],
                                start: Tuple[float, float, float],
                                end: Tuple[float, float, float]) -> float:
    """
    Calculate the perpendicular distance from a point to a line segment.

    This is the distance from the point to the closest point on the line
    connecting start and end.
    """
    # Vector from start to end
    line_vec = (end[0] - start[0], end[1] - start[1], end[2] - start[2])
    line_length_sq = line_vec[0]**2 + line_vec[1]**2 + line_vec[2]**2

    if line_length_sq == 0:
        # Start and end are the same point
        return calculate_distance(point, start)

    # Vector from start to point
    point_vec = (point[0] - start[0], point[1] - start[1], point[2] - start[2])

    # Project point onto line (parameter t in [0, 1] for segment)
    t = (point_vec[0] * line_vec[0] + point_vec[1] * line_vec[1] +
         point_vec[2] * line_vec[2]) / line_length_sq

    # Clamp t to [0, 1] to stay on the segment
    t = max(0, min(1, t))

    # Find closest point on segment
    closest = (
        start[0] + t * line_vec[0],
        start[1] + t * line_vec[1],
        start[2] + t * line_vec[2]
    )

    # Return distance from point to closest point
    return calculate_distance(point, closest)


def load_systems(input_file: Path) -> Tuple[List[Dict], List[str], List[str]]:
    """Load systems from TSV file.

    Returns (systems, body_type_columns, all_fieldnames).
    Each system dict includes parsed coordinates plus a '_row' key holding
    the original TSV row dict for pass-through output.
    """
    systems = []
    body_type_columns = []
    all_fieldnames = []

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')

        # Identify body type columns (those ending in _count)
        if reader.fieldnames:
            all_fieldnames = list(reader.fieldnames)
            body_type_columns = [col for col in reader.fieldnames
                                if col.endswith('_count') and col != 'total_bodies']

        for row in reader:
            try:
                system = {
                    'name': row['name'],
                    'x': float(row['x']),
                    'y': float(row['y']),
                    'z': float(row['z']),
                    'position': (float(row['x']), float(row['y']), float(row['z'])),
                    '_row': dict(row),  # preserve full original row
                }

                # Load body type counts
                for col in body_type_columns:
                    try:
                        system[col] = int(row[col]) if row[col] else 0
                    except (ValueError, KeyError):
                        system[col] = 0

                systems.append(system)
            except (KeyError, ValueError) as e:
                print(f"Warning: Skipping invalid row: {e}", file=sys.stderr)
                continue

    return systems, body_type_columns, all_fieldnames


SPECIES_VALUE_SUFFIX = re.compile(r'\s+[\d.]+M$')


def body_species(body_details: str) -> List[Tuple[str, List[str]]]:
    """Body names and predicted species from a find_high_value_systems body_details field.

    The field looks like "Body A(Species 1 19.0M, Species 2 16.2M);Body B(Species 3 12.9M)".
    The credit value is stripped from each species. An entry without a parenthesised
    species list gives the whole entry as the body name and no species.
    """
    bodies = []
    for entry in body_details.split(';'):
        entry = entry.strip()
        if not entry:
            continue
        open_idx = entry.rfind('(')
        if open_idx == -1 or not entry.endswith(')'):
            bodies.append((entry, []))
            continue
        species = [SPECIES_VALUE_SUFFIX.sub('', item.strip())
                   for item in entry[open_idx + 1:-1].split(',') if item.strip()]
        bodies.append((entry[:open_idx].strip(), species))
    return bodies


def body_signal_counts(body_details: str) -> List[int]:
    """Number of predicted species on each body in a body_details field (see body_species)."""
    return [len(species) for _, species in body_species(body_details)]


def filter_by_body_signals(systems: List[Dict], min_signals: int, mode: str) -> List[Dict]:
    """Keep systems whose bodies have at least min_signals predicted species.

    mode 'any': at least one body qualifies; mode 'all': every listed body qualifies.
    Systems without body_details are dropped.
    """
    check = any if mode == 'any' else all
    kept = []
    for system in systems:
        counts = body_signal_counts(system['_row'].get('body_details') or '')
        if counts and check(c >= min_signals for c in counts):
            kept.append(system)
    return kept


def resolve_species_targets(species: List[str], genera: List[str]) -> List[Set[str]]:
    """Turn --species and --genus names into target groups of canonical species names.

    Each listed species is a group of one; each listed genus is a group holding all of
    its species. Names are case-insensitive. Raises ValueError for an unknown name.
    """
    all_species = [entry['name'] for entry in HIGH_VALUE_SPECIES]
    by_species = {name.lower(): name for name in all_species}
    by_genus: Dict[str, Set[str]] = {}
    for name in all_species:
        by_genus.setdefault(name.split()[0].lower(), set()).add(name)
    genus_names = sorted({name.split()[0] for name in all_species})

    def unknown(kind, name, choices):
        close = difflib.get_close_matches(name, choices, n=3, cutoff=0.6)
        hint = f" Did you mean: {', '.join(close)}?" if close else ""
        return ValueError(f"Unknown {kind} '{name}'.{hint} Known: {', '.join(choices)}")

    groups = []
    for name in species:
        key = name.strip().lower()
        if key not in by_species:
            raise unknown('species', name.strip(), sorted(all_species))
        groups.append({by_species[key]})
    for name in genera:
        key = name.strip().lower()
        if key not in by_genus:
            raise unknown('genus', name.strip(), genus_names)
        groups.append(by_genus[key])
    return groups


def filter_by_species(systems: List[Dict], groups: List[Set[str]], match: str,
                      scope: str) -> List[Dict]:
    """Keep systems with the target species predicted.

    match 'any': at least one target group is predicted; 'all': every group is.
    scope 'body': the match must hold on a single body; 'system': anywhere in the system.
    Kept systems get a matched_species field listing each body's target species.
    Systems without body_details are dropped.
    """
    targets = set().union(*groups)
    check = any if match == 'any' else all

    def satisfied(predicted: Set[str]) -> bool:
        return check(group & predicted for group in groups)

    kept = []
    for system in systems:
        bodies = body_species(system['_row'].get('body_details') or '')
        if scope == 'body':
            ok = any(satisfied(set(species)) for _, species in bodies)
        else:
            ok = satisfied({sp for _, species in bodies for sp in species})
        if not ok:
            continue
        matched = [(body, [sp for sp in species if sp in targets]) for body, species in bodies]
        system['_row']['matched_species'] = ';'.join(
            f"{body}({', '.join(species)})" for body, species in matched if species)
        kept.append(system)
    return kept


def cluster_systems(systems: List[Dict], max_distance: float) -> Dict[int, List[Dict]]:
    """
    Cluster systems using KDTree + union-find algorithm.

    Uses scipy KDTree for O(n log n) spatial indexing instead of O(n^2)
    brute-force, enabling clustering of 100k+ systems.

    Returns a dictionary mapping cluster ID to list of systems in that cluster.
    """
    n = len(systems)
    if n == 0:
        return {}

    uf = UnionFind(n)

    # Build KDTree for efficient neighbor queries
    coords = np.array([[s['x'], s['y'], s['z']] for s in systems])
    tree = KDTree(coords)

    # Find all pairs within max_distance — O(n log n + k) vs O(n^2)
    pairs = tree.query_pairs(max_distance)
    for i, j in pairs:
        uf.union(i, j)

    # Group systems by their cluster root
    clusters: Dict[int, List[int]] = {}
    for i in range(n):
        root = uf.find(i)
        if root not in clusters:
            clusters[root] = []
        clusters[root].append(i)

    # Convert to system dictionaries
    result = {}
    for cluster_id, indices in clusters.items():
        result[cluster_id] = [systems[i] for i in indices]

    return result


def cluster_systems_dbscan(
    systems: List[Dict], eps: float, min_samples: int
) -> Dict[int, List[Dict]]:
    """
    Cluster systems using DBSCAN.

    Unlike single-linkage union-find, DBSCAN requires a minimum density
    (min_samples neighbors within eps) to form a cluster core, preventing
    long chains across sparse regions. Noise points (no dense neighborhood)
    are returned as singleton clusters with negative IDs.

    Args:
        systems: List of system dicts with 'x', 'y', 'z' keys.
        eps: Maximum distance between neighbors (LY).
        min_samples: Minimum systems within eps to form a core point.

    Returns:
        Dictionary mapping cluster ID to list of systems.
    """
    if not systems:
        return {}

    coords = np.array([[s['x'], s['y'], s['z']] for s in systems])

    db = DBSCAN(eps=eps, min_samples=min_samples, algorithm='ball_tree',
                metric='euclidean', n_jobs=-1)
    labels = db.fit_predict(coords)

    clusters: Dict[int, List[Dict]] = {}
    noise_id = -1
    for i, label in enumerate(labels):
        if label == -1:
            # Each noise point gets its own singleton cluster
            clusters[noise_id] = [systems[i]]
            noise_id -= 1
        else:
            clusters.setdefault(label, []).append(systems[i])

    return clusters


def cluster_systems_hdbscan(
    systems: List[Dict], min_cluster_size: int, min_samples: int
) -> Dict[int, List[Dict]]:
    """Cluster systems using HDBSCAN.

    HDBSCAN finds clusters at varying density scales — dense cores in the
    galactic plane won't swallow sparse but real groups elsewhere. Each point
    gets a membership probability; noise points are returned as singletons.

    Typically much faster than DBSCAN on large datasets (O(n log n) with
    spatial indexing vs DBSCAN's expensive neighbor graph at large eps).

    Args:
        systems: List of system dicts with 'x', 'y', 'z' keys.
        min_cluster_size: Smallest group size to consider a cluster.
        min_samples: Core point density threshold (higher = more conservative).

    Returns:
        Dictionary mapping cluster ID to list of systems.
    """
    if not systems:
        return {}

    coords = np.array([[s['x'], s['y'], s['z']] for s in systems])

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric='euclidean',
        core_dist_n_jobs=-1,
    )
    labels = clusterer.fit_predict(coords)

    clusters: Dict[int, List[Dict]] = {}
    noise_id = -1
    for i, label in enumerate(labels):
        if label == -1:
            clusters[noise_id] = [systems[i]]
            noise_id -= 1
        else:
            clusters.setdefault(label, []).append(systems[i])

    return clusters


def calculate_cluster_stats(cluster: List[Dict], body_type_columns: List[str],
                           corridor_start: Tuple[float, float, float] = None,
                           corridor_end: Tuple[float, float, float] = None) -> Dict:
    """Calculate statistics for a cluster."""
    if not cluster:
        return {}

    # Calculate centroid
    centroid_x = sum(s['x'] for s in cluster) / len(cluster)
    centroid_y = sum(s['y'] for s in cluster) / len(cluster)
    centroid_z = sum(s['z'] for s in cluster) / len(cluster)
    centroid = (centroid_x, centroid_y, centroid_z)

    # Calculate max distance from centroid
    max_radius = 0.0
    for system in cluster:
        dist = calculate_distance(centroid, system['position'])
        max_radius = max(max_radius, dist)

    # Calculate max pairwise distance
    max_diameter = 0.0
    for i, sys1 in enumerate(cluster):
        for sys2 in cluster[i+1:]:
            dist = calculate_distance(sys1['position'], sys2['position'])
            max_diameter = max(max_diameter, dist)

    # Calculate corridor distance if coordinates provided
    corridor_distance = None
    if corridor_start is not None and corridor_end is not None:
        corridor_distance = calculate_corridor_distance(centroid, corridor_start, corridor_end)

    # Aggregate body type counts
    body_counts = {}
    for col in body_type_columns:
        # Create a list of counts per system for this body type
        counts_per_system = [s.get(col, 0) for s in cluster]
        body_counts[col] = counts_per_system

    # Get system names
    system_names = [s['name'] for s in cluster]

    return {
        'size': len(cluster),
        'centroid': centroid,
        'max_radius': max_radius,
        'max_diameter': max_diameter,
        'corridor_distance': corridor_distance,
        'system_names': system_names,
        'body_counts': body_counts
    }


def write_clusters(clusters: Dict[int, List[Dict]], output_file: Path,
                   body_type_columns: List[str],
                   corridor_start: Tuple[float, float, float] = None,
                   corridor_end: Tuple[float, float, float] = None):
    """Write clusters to TSV file (one line per cluster)."""
    # Sort clusters by size (descending)
    sorted_clusters = sorted(clusters.items(),
                            key=lambda x: len(x[1]),
                            reverse=True)

    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')

        # Build header
        header = [
            'cluster_id', 'cluster_size',
            'centroid_x', 'centroid_y', 'centroid_z',
            'max_radius', 'max_diameter'
        ]

        # Add corridor distance if applicable
        if corridor_start is not None and corridor_end is not None:
            header.append('corridor_distance')

        # Add body type columns
        for col in body_type_columns:
            # Remove _count suffix for cleaner column names
            body_type = col.replace('_count', '')
            header.append(f'{body_type}_per_system')

        # Add systems column last
        header.append('systems')

        writer.writerow(header)

        # Write cluster data
        for cluster_num, (cluster_id, cluster_systems) in enumerate(sorted_clusters, 1):
            stats = calculate_cluster_stats(cluster_systems, body_type_columns,
                                           corridor_start, corridor_end)

            row = [
                cluster_num,
                stats['size'],
                f"{stats['centroid'][0]:.5f}",
                f"{stats['centroid'][1]:.5f}",
                f"{stats['centroid'][2]:.5f}",
                f"{stats['max_radius']:.2f}",
                f"{stats['max_diameter']:.2f}"
            ]

            # Add corridor distance if calculated
            if stats['corridor_distance'] is not None:
                row.append(f"{stats['corridor_distance']:.2f}")

            # Add body counts as comma-separated lists
            for col in body_type_columns:
                counts = stats['body_counts'][col]
                counts_str = ','.join(str(c) for c in counts)
                row.append(counts_str)

            # Add system names as semicolon-separated list
            systems_str = '; '.join(stats['system_names'])
            row.append(systems_str)

            writer.writerow(row)


def write_summary(clusters: Dict[int, List[Dict]], summary_file: Path,
                 body_type_columns: List[str],
                 max_distance: float, total_systems: int,
                 corridor_start: Tuple[float, float, float] = None,
                 corridor_end: Tuple[float, float, float] = None,
                 filter_notes: List[str] = None):
    """Write clustering summary."""
    sorted_clusters = sorted(clusters.items(),
                            key=lambda x: len(x[1]),
                            reverse=True)

    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"Clustering Summary\n")
        f.write(f"{'=' * 80}\n\n")
        if max_distance is not None:
            f.write(f"Max Distance: {max_distance:.2f} LY\n")
        else:
            f.write(f"Method: HDBSCAN (adaptive density)\n")
        for note in filter_notes or []:
            f.write(f"{note}\n")
        f.write(f"Total Systems: {total_systems}\n")
        f.write(f"Total Clusters: {len(clusters)}\n")
        f.write(f"Isolated Systems (size 1): {sum(1 for c in clusters.values() if len(c) == 1)}\n")
        f.write(f"Multi-System Clusters: {sum(1 for c in clusters.values() if len(c) > 1)}\n")

        if corridor_start is not None and corridor_end is not None:
            f.write(f"\nCorridor: ({corridor_start[0]:.1f}, {corridor_start[1]:.1f}, {corridor_start[2]:.1f}) to "
                   f"({corridor_end[0]:.1f}, {corridor_end[1]:.1f}, {corridor_end[2]:.1f})\n")
            corridor_length = calculate_distance(corridor_start, corridor_end)
            f.write(f"Corridor Length: {corridor_length:.2f} LY\n")

        f.write("\n")

        # Statistics
        cluster_sizes = [len(c) for c in clusters.values()]
        if cluster_sizes:
            f.write(f"Cluster Size Statistics:\n")
            f.write(f"  Largest: {max(cluster_sizes)}\n")
            f.write(f"  Average: {sum(cluster_sizes) / len(cluster_sizes):.2f}\n")
            f.write(f"  Median: {sorted(cluster_sizes)[len(cluster_sizes)//2]}\n\n")

        # Top clusters
        header_line = f"{'Rank':<6} {'Size':<8} {'Centroid (x, y, z)':<40} {'Radius':<10} {'Diameter':<10}"
        if corridor_start is not None:
            header_line += f" {'CorridorDist':<12}"
        f.write(f"Top 20 Clusters by Size:\n")
        f.write(f"{'-' * len(header_line)}\n")
        f.write(f"{header_line}\n")
        f.write(f"{'-' * len(header_line)}\n")

        for rank, (cluster_id, cluster_systems) in enumerate(sorted_clusters[:20], 1):
            stats = calculate_cluster_stats(cluster_systems, body_type_columns,
                                           corridor_start, corridor_end)
            centroid_str = f"({stats['centroid'][0]:.1f}, {stats['centroid'][1]:.1f}, {stats['centroid'][2]:.1f})"
            line = f"{rank:<6} {stats['size']:<8} {centroid_str:<40} " \
                   f"{stats['max_radius']:<10.2f} {stats['max_diameter']:<10.2f}"
            if stats['corridor_distance'] is not None:
                line += f" {stats['corridor_distance']:<12.2f}"
            f.write(f"{line}\n")

        f.write(f"\nCluster Distribution:\n")
        f.write(f"{'-' * 40}\n")

        # Size distribution
        size_dist = {}
        for cluster in clusters.values():
            size = len(cluster)
            size_dist[size] = size_dist.get(size, 0) + 1

        for size in sorted(size_dist.keys(), reverse=True)[:10]:
            f.write(f"  Size {size:3d}: {size_dist[size]:4d} clusters\n")


def greedy_nearest_neighbor_order(systems: List[Dict]) -> List[Dict]:
    """Order systems by greedy nearest-neighbor traversal.

    Starts from the system closest to galactic center (0,0,0) and
    greedily picks the nearest unvisited system at each step.
    """
    if len(systems) <= 1:
        return list(systems)

    coords = np.array([[s['x'], s['y'], s['z']] for s in systems])

    # Start from system closest to origin
    dists_to_origin = np.sum(coords ** 2, axis=1)
    current = int(np.argmin(dists_to_origin))

    ordered = [systems[current]]
    visited = {current}
    n = len(systems)

    for _ in range(n - 1):
        # Distances from current to all others
        diff = coords - coords[current]
        dists = np.sum(diff ** 2, axis=1)
        dists[list(visited)] = np.inf
        nearest = int(np.argmin(dists))
        ordered.append(systems[nearest])
        visited.add(nearest)
        current = nearest

    return ordered


def _sanitize_filename(name: str, max_len: int = 40) -> str:
    """Sanitize a system name for use in a filename."""
    s = re.sub(r'[^\w\s\-]', '', name)
    s = re.sub(r'\s+', '_', s.strip())
    return s[:max_len] if s else 'unknown'


def write_cluster_directory(
    clusters: Dict[int, List[Dict]],
    output_file: Path,
    all_fieldnames: List[str],
    body_type_columns: List[str],
):
    """Write per-cluster TSV files into a clusters/ subdirectory.

    Each multi-system cluster gets its own TSV with systems in
    nearest-neighbor order and a jump_distance column.
    Also writes a clusters_index.tsv summarising all clusters.
    """
    clusters_dir = output_file.parent / 'clusters'
    clusters_dir.mkdir(parents=True, exist_ok=True)

    # Sort clusters by size descending, skip singletons
    sorted_clusters = sorted(
        ((cid, members) for cid, members in clusters.items() if len(members) > 1),
        key=lambda x: len(x[1]),
        reverse=True,
    )

    if not sorted_clusters:
        print("No multi-system clusters to write.")
        return

    # Output fieldnames = original columns + jump_distance
    out_fields = list(all_fieldnames) + ['jump_distance']

    index_rows = []

    for rank, (cluster_id, members) in enumerate(sorted_clusters, 1):
        ordered = greedy_nearest_neighbor_order(members)

        # Compute jump distances
        jump_distances = []
        for i in range(len(ordered) - 1):
            d = calculate_distance(ordered[i]['position'], ordered[i + 1]['position'])
            jump_distances.append(f"{d:.2f}")
        jump_distances.append('')  # last system has no next jump

        # Build filename
        first_name = ordered[0]['name']
        safe_name = _sanitize_filename(first_name)
        filename = f"cluster_{rank:04d}_{len(ordered)}sys_{safe_name}.tsv"
        filepath = clusters_dir / filename

        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=out_fields, delimiter='\t',
                                    extrasaction='ignore')
            writer.writeheader()
            for sys_dict, jd in zip(ordered, jump_distances):
                row = dict(sys_dict['_row'])
                row['jump_distance'] = jd
                writer.writerow(row)

        # Cluster stats for index
        stats = calculate_cluster_stats(members, body_type_columns)
        index_rows.append({
            'rank': rank,
            'size': len(ordered),
            'centroid_x': f"{stats['centroid'][0]:.2f}",
            'centroid_y': f"{stats['centroid'][1]:.2f}",
            'centroid_z': f"{stats['centroid'][2]:.2f}",
            'diameter': f"{stats['max_diameter']:.2f}",
            'first_system': ordered[0]['name'],
            'filename': filename,
        })

    # Write index
    index_path = clusters_dir / 'clusters_index.tsv'
    index_fields = ['rank', 'size', 'centroid_x', 'centroid_y', 'centroid_z',
                    'diameter', 'first_system', 'filename']
    with open(index_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=index_fields, delimiter='\t')
        writer.writeheader()
        writer.writerows(index_rows)

    print(f"Wrote {len(sorted_clusters)} cluster files to: {clusters_dir}")
    print(f"Cluster index: {index_path}")


def parse_coordinates(coord_str: str) -> Tuple[float, float, float]:
    """Parse coordinate string in format 'x,y,z' to tuple."""
    try:
        parts = coord_str.split(',')
        if len(parts) != 3:
            raise ValueError("Coordinates must be in format 'x,y,z'")
        return (float(parts[0]), float(parts[1]), float(parts[2]))
    except Exception as e:
        raise ValueError(f"Invalid coordinate format: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Cluster systems based on proximity',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Cluster systems within 50 LY of each other
  %(prog)s -i results.tsv -d 50 -o clusters.tsv

  # Cluster with corridor distance calculation
  %(prog)s -i results.tsv -d 50 -o clusters.tsv \\
    --start "-468,-92,4474" --end "-575,-37,5142"

  # Only systems where at least one body has 2+ predicted species
  %(prog)s -i results.tsv -d 50 -o clusters.tsv --min-body-signals 2

  # Only systems where every predicted body has 2+ predicted species
  %(prog)s -i results.tsv -d 50 -o clusters.tsv --min-body-signals 2 --signal-filter all

  # Only systems with a body predicted to host Stratum Tectonicas or any Recepta
  %(prog)s -i results.tsv -d 50 -o clusters.tsv --species "Stratum Tectonicas" --genus Recepta

  # Only systems with a body predicted to host both Tubus Cavas and Clypeus Speculumi
  %(prog)s -i results.tsv -d 50 -o clusters.tsv \\
    --species "Tubus Cavas,Clypeus Speculumi" --species-match all
        """
    )

    parser.add_argument('-i', '--input', type=Path, required=True,
                       help='Input TSV file with system data')
    parser.add_argument('-d', '--distance', type=float, default=None,
                       help='Maximum distance (LY) for clustering '
                            '(required for union-find and dbscan)')
    parser.add_argument('-o', '--output', type=Path, required=True,
                       help='Output TSV file for cluster results')
    parser.add_argument('-s', '--summary', type=Path,
                       help='Output summary file (default: <output>_summary.txt)')
    parser.add_argument('-m', '--method', choices=['union-find', 'dbscan', 'hdbscan'],
                       default='union-find',
                       help='Clustering method (default: union-find)')
    parser.add_argument('--min-samples', type=int, default=3,
                       help='DBSCAN/HDBSCAN min_samples: core point density '
                            'threshold (default: 3)')
    parser.add_argument('--min-cluster-size', type=int, default=5,
                       help='HDBSCAN min_cluster_size: smallest group to '
                            'consider a cluster (default: 5)')
    parser.add_argument('--start', type=str,
                       help='Corridor start coordinates (format: "x,y,z")')
    parser.add_argument('--end', type=str,
                       help='Corridor end coordinates (format: "x,y,z")')
    parser.add_argument('--min-body-signals', type=int, default=None,
                       help='Before clustering, keep only systems whose bodies have at least '
                            'this many predicted species (signals), counted from the '
                            'body_details column of find_high_value_systems.py output')
    parser.add_argument('--signal-filter', choices=['any', 'all'], default='any',
                       help="With --min-body-signals: 'any' keeps a system if at least one body "
                            "qualifies (default); 'all' requires every predicted body to qualify")
    parser.add_argument('--species', type=str, default=None,
                       help='Before clustering, keep only systems with these predicted species '
                            '(comma-separated, case-insensitive), from body_details')
    parser.add_argument('--genus', type=str, default=None,
                       help='Like --species, but each genus matches any of its species '
                            '(comma-separated, e.g. "Recepta,Clypeus")')
    parser.add_argument('--species-match', choices=['any', 'all'], default='any',
                       help="With --species/--genus: 'any' needs at least one listed species or "
                            "genus (default); 'all' needs every one")
    parser.add_argument('--species-scope', choices=['body', 'system'], default='body',
                       help="With --species/--genus: 'body' needs the match on a single body "
                            "(default); 'system' allows it to be spread across the system's bodies")

    args = parser.parse_args()

    if args.min_body_signals is not None and args.min_body_signals < 1:
        parser.error("--min-body-signals must be at least 1")

    split_names = lambda value: [n for n in (value or '').split(',') if n.strip()]
    species_groups = None
    if args.species or args.genus:
        try:
            species_groups = resolve_species_targets(split_names(args.species),
                                                     split_names(args.genus))
        except ValueError as e:
            parser.error(str(e))
        if not species_groups:
            parser.error("--species/--genus needs at least one name")

    if args.method != 'hdbscan' and args.distance is None:
        parser.error("--distance is required for union-find and dbscan methods")

    # Parse corridor coordinates if provided
    corridor_start = None
    corridor_end = None
    if args.start or args.end:
        if not (args.start and args.end):
            parser.error("Both --start and --end must be provided for corridor distance calculation")
        corridor_start = parse_coordinates(args.start)
        corridor_end = parse_coordinates(args.end)

    # Set default summary file if not specified
    if args.summary is None:
        args.summary = args.output.parent / f"{args.output.stem}_summary.txt"

    # Create output directory if needed
    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(f"Loading systems from: {args.input}")
    systems, body_type_columns, all_fieldnames = load_systems(args.input)
    print(f"Loaded {len(systems)} systems")
    print(f"Body type columns: {', '.join(body_type_columns)}")

    filter_notes = []
    if (args.min_body_signals is not None or species_groups) and 'body_details' not in all_fieldnames:
        parser.error("--min-body-signals, --species and --genus need a body_details column "
                     "(output of find_high_value_systems.py)")

    if args.min_body_signals is not None:
        loaded = len(systems)
        systems = filter_by_body_signals(systems, args.min_body_signals, args.signal_filter)
        which = 'at least one body' if args.signal_filter == 'any' else 'every predicted body'
        note = (f"{which} with >= {args.min_body_signals} predicted species; "
                f"kept {len(systems)} of {loaded} systems")
        print(f"Signal filter: {note}")
        filter_notes.append(f"Signal filter: {note}")

    if species_groups:
        loaded = len(systems)
        systems = filter_by_species(systems, species_groups, args.species_match,
                                    args.species_scope)
        listed = split_names(args.species) + [f"{g.strip()} (genus)" for g in split_names(args.genus)]
        which = 'any of' if args.species_match == 'any' else 'all of'
        where = 'on a single body' if args.species_scope == 'body' else 'anywhere in the system'
        note = (f"{which} {', '.join(n.strip() for n in listed)} {where}; "
                f"kept {len(systems)} of {loaded} systems")
        print(f"Species filter: {note}")
        filter_notes.append(f"Species filter: {note}")
        all_fieldnames = list(all_fieldnames) + ['matched_species']

    if filter_notes and not systems:
        print("No systems pass the filters; nothing to cluster.")
        return

    if corridor_start and corridor_end:
        corridor_length = calculate_distance(corridor_start, corridor_end)
        print(f"\nCorridor: {corridor_start} to {corridor_end}")
        print(f"Corridor length: {corridor_length:.2f} LY")

    print(f"\nClustering with method: {args.method}")
    if args.method == 'hdbscan':
        print(f"HDBSCAN min_cluster_size: {args.min_cluster_size}, min_samples: {args.min_samples}")
        clusters = cluster_systems_hdbscan(systems, args.min_cluster_size, args.min_samples)
    elif args.method == 'dbscan':
        print(f"DBSCAN eps: {args.distance:.2f} LY, min_samples: {args.min_samples}")
        clusters = cluster_systems_dbscan(systems, args.distance, args.min_samples)
    else:
        print(f"Union-find max distance: {args.distance:.2f} LY")
        clusters = cluster_systems(systems, args.distance)

    print(f"Found {len(clusters)} clusters")
    multi_clusters = sum(1 for c in clusters.values() if len(c) > 1)
    print(f"  - Multi-system clusters: {multi_clusters}")
    print(f"  - Isolated systems: {len(clusters) - multi_clusters}")

    if clusters:
        largest = max(len(c) for c in clusters.values())
        print(f"  - Largest cluster: {largest} systems")

    print(f"\nWriting clusters to: {args.output}")
    write_clusters(clusters, args.output, body_type_columns, corridor_start, corridor_end)

    print(f"Writing summary to: {args.summary}")
    write_summary(clusters, args.summary, body_type_columns, args.distance,
                 len(systems), corridor_start, corridor_end, filter_notes)

    print(f"\nWriting per-cluster files...")
    write_cluster_directory(clusters, args.output, all_fieldnames, body_type_columns)

    print("\nDone!")


if __name__ == '__main__':
    main()
