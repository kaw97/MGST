#!/usr/bin/env python3
"""Find systems within a specified distance of faction-controlled systems.

This script takes two TSV files containing system coordinates and finds systems
from the first file that are within a specified distance of any system in the
second file.

Usage:
    python find_nearby_systems.py <elw_systems.tsv> <faction_systems.tsv> <distance_ly>
"""

import sys
import csv
import math
from pathlib import Path
from typing import List, Tuple, Dict
from multiprocessing import Pool, cpu_count


def parse_coordinates(coord_str: str) -> Tuple[float, float, float]:
    """Parse coordinate string in format 'x, y, z' to tuple of floats.

    Args:
        coord_str: Coordinate string in format 'x, y, z'

    Returns:
        Tuple of (x, y, z) coordinates
    """
    parts = coord_str.strip().split(',')
    if len(parts) != 3:
        raise ValueError(f"Invalid coordinate format: {coord_str}")
    return (float(parts[0].strip()), float(parts[1].strip()), float(parts[2].strip()))


def calculate_distance(coord1: Tuple[float, float, float],
                       coord2: Tuple[float, float, float]) -> float:
    """Calculate 3D Euclidean distance between two points.

    Args:
        coord1: First coordinate (x, y, z)
        coord2: Second coordinate (x, y, z)

    Returns:
        Distance in light years
    """
    dx = coord2[0] - coord1[0]
    dy = coord2[1] - coord1[1]
    dz = coord2[2] - coord1[2]
    return math.sqrt(dx**2 + dy**2 + dz**2)


def read_systems_tsv(tsv_file: Path) -> List[Dict[str, str]]:
    """Read systems from TSV file.

    Args:
        tsv_file: Path to TSV file

    Returns:
        List of system dictionaries with 'name' and coordinates
    """
    systems = []
    with open(tsv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            systems.append({
                'name': row['name'],
                'x': float(row['x']),
                'y': float(row['y']),
                'z': float(row['z']),
                'total_bodies': int(row.get('total_bodies', 0) or 0),
                'elw_count': int(row.get('Earth-like world_count', 0) or 0),
                'water_world_count': int(row.get('Water world_count', 0) or 0),
                'ammonia_world_count': int(row.get('Ammonia world_count', 0) or 0),
                'black_hole_count': int(row.get('Black Hole_count', 0) or 0),
                'neutron_star_count': int(row.get('Neutron Star_count', 0) or 0)
            })
    return systems


def process_target_chunk(args):
    """Process a chunk of target systems (worker function for multiprocessing).

    Args:
        args: Tuple of (target_systems_chunk, reference_coords, max_distance)

    Returns:
        List of matching systems within max_distance
    """
    target_chunk, reference_coords, max_distance = args
    results = []

    for target_sys in target_chunk:
        target_coords = (target_sys['x'], target_sys['y'], target_sys['z'])

        # Find nearest reference system
        nearest_ref = None
        nearest_distance = float('inf')

        for ref_sys, ref_coords in reference_coords:
            distance = calculate_distance(target_coords, ref_coords)
            if distance < nearest_distance:
                nearest_distance = distance
                nearest_ref = ref_sys

        # If within max distance, add to results
        if nearest_distance <= max_distance and nearest_ref is not None:
            results.append((target_sys, nearest_ref, nearest_distance))

    return results


def find_nearby_systems(target_systems: List[Dict[str, str]],
                       reference_systems: List[Dict[str, str]],
                       max_distance: float,
                       num_workers: int = None) -> List[Tuple[Dict[str, str], Dict[str, str], float]]:
    """Find target systems within max_distance of any reference system.

    Args:
        target_systems: Systems to check (e.g., ELW systems)
        reference_systems: Reference systems (e.g., faction-controlled systems)
        max_distance: Maximum distance in light years
        num_workers: Number of parallel workers (default: CPU count)

    Returns:
        List of tuples (target_system, nearest_reference_system, distance)
    """
    if num_workers is None:
        num_workers = cpu_count()

    # Extract reference system coordinates
    reference_coords = []
    for ref_sys in reference_systems:
        coords = (ref_sys['x'], ref_sys['y'], ref_sys['z'])
        reference_coords.append((ref_sys, coords))

    # Split target systems into chunks for parallel processing
    chunk_size = max(1, len(target_systems) // num_workers)
    chunks = []
    for i in range(0, len(target_systems), chunk_size):
        chunk = target_systems[i:i + chunk_size]
        chunks.append((chunk, reference_coords, max_distance))

    print(f"Processing {len(target_systems)} systems with {num_workers} workers ({len(chunks)} chunks)...", file=sys.stderr)

    # Process chunks in parallel
    with Pool(num_workers) as pool:
        chunk_results = pool.map(process_target_chunk, chunks)

    # Combine results from all chunks
    results = []
    for chunk_result in chunk_results:
        results.extend(chunk_result)

    return results


def main():
    """Main entry point."""
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage: find_nearby_systems.py <target_systems.tsv> <reference_systems.tsv> <max_distance_ly> [num_workers]",
              file=sys.stderr)
        sys.exit(1)

    target_file = Path(sys.argv[1])
    reference_file = Path(sys.argv[2])
    max_distance = float(sys.argv[3])
    num_workers = int(sys.argv[4]) if len(sys.argv) == 5 else None

    # Validate files exist
    if not target_file.exists():
        print(f"Error: Target file not found: {target_file}", file=sys.stderr)
        sys.exit(1)
    if not reference_file.exists():
        print(f"Error: Reference file not found: {reference_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading target systems from {target_file}...", file=sys.stderr)
    target_systems = read_systems_tsv(target_file)
    print(f"  Loaded {len(target_systems)} target systems", file=sys.stderr)

    print(f"Loading reference systems from {reference_file}...", file=sys.stderr)
    reference_systems = read_systems_tsv(reference_file)
    print(f"  Loaded {len(reference_systems)} reference systems", file=sys.stderr)

    print(f"Finding target systems within {max_distance} LY of reference systems...", file=sys.stderr)
    results = find_nearby_systems(target_systems, reference_systems, max_distance, num_workers)

    print(f"\nFound {len(results)} target systems within {max_distance} LY\n", file=sys.stderr)

    # Output results as TSV
    # LTT 1873 coordinates
    ltt_1873_coords = (104.28, -127.19, -49.22)

    print("Target System\tTotal Bodies\tEarth-like Worlds\tWater Worlds\tAmmonia Worlds\tBlack Holes\tNeutron Stars\tNearest Reference System\tDistance to Reference (LY)\tDistance from Sol (LY)\tDistance from LTT 1873 (LY)")
    results.sort(key=lambda x: x[2])  # Sort by distance
    for target_sys, ref_sys, distance in results:
        # Calculate distance from origin (Sol at 0,0,0)
        dist_from_sol = math.sqrt(target_sys['x']**2 + target_sys['y']**2 + target_sys['z']**2)
        # Calculate distance from LTT 1873
        target_coords = (target_sys['x'], target_sys['y'], target_sys['z'])
        dist_from_ltt1873 = calculate_distance(target_coords, ltt_1873_coords)
        print(f"{target_sys['name']}\t{target_sys['total_bodies']}\t{target_sys['elw_count']}\t{target_sys['water_world_count']}\t{target_sys['ammonia_world_count']}\t{target_sys['black_hole_count']}\t{target_sys['neutron_star_count']}\t{ref_sys['name']}\t{distance:.2f}\t{dist_from_sol:.2f}\t{dist_from_ltt1873:.2f}")


if __name__ == '__main__':
    main()
