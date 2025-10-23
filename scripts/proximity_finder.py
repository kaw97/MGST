#!/usr/bin/env python3
"""
Proximity Finder Script

Finds all systems in a binary systems file that are within a specified distance
of at least one system in a faction systems file using multiprocessing.

Usage:
    python proximity_finder.py <binary_file> <faction_file> <max_distance> <output_file>
"""

import pandas as pd
import numpy as np
import multiprocessing as mp
from functools import partial
import sys
import time
from pathlib import Path


def calculate_distance(coords1, coords2):
    """Calculate 3D Euclidean distance between two coordinate sets."""
    return np.sqrt(
        (coords1[0] - coords2[0]) ** 2 +
        (coords1[1] - coords2[1]) ** 2 +
        (coords1[2] - coords2[2]) ** 2
    )


def find_nearby_systems_chunk(binary_chunk, faction_coords, max_distance):
    """Process a chunk of binary systems to find those within max_distance of any faction system."""
    nearby_systems = []

    for idx, binary_system in binary_chunk.iterrows():
        binary_coords = (binary_system['coords_x'], binary_system['coords_y'], binary_system['coords_z'])

        # Check distance to all faction systems
        min_distance = float('inf')
        closest_faction_system = None

        for faction_idx, faction_coords_row in faction_coords.iterrows():
            faction_coord = (faction_coords_row['coords_x'], faction_coords_row['coords_y'], faction_coords_row['coords_z'])
            distance = calculate_distance(binary_coords, faction_coord)

            if distance < min_distance:
                min_distance = distance
                closest_faction_system = faction_coords_row['system_name']

            # Early exit if we find a system within range
            if distance <= max_distance:
                break

        if min_distance <= max_distance:
            # Add proximity information to the binary system data
            result_row = binary_system.copy()
            result_row['closest_faction_system'] = closest_faction_system
            result_row['distance_to_faction'] = min_distance
            nearby_systems.append(result_row)

    return nearby_systems


def split_dataframe(df, num_chunks):
    """Split dataframe into roughly equal chunks."""
    chunk_size = len(df) // num_chunks
    remainder = len(df) % num_chunks

    chunks = []
    start = 0

    for i in range(num_chunks):
        # Add one extra row to some chunks to handle remainder
        size = chunk_size + (1 if i < remainder else 0)
        end = start + size
        chunks.append(df.iloc[start:end])
        start = end

    return chunks


def main():
    if len(sys.argv) != 5:
        print("Usage: python proximity_finder.py <binary_file> <faction_file> <max_distance> <output_file>")
        sys.exit(1)

    binary_file = sys.argv[1]
    faction_file = sys.argv[2]
    max_distance = float(sys.argv[3])
    output_file = sys.argv[4]

    print(f"🔍 Proximity Analysis")
    print(f"Binary systems: {binary_file}")
    print(f"Faction systems: {faction_file}")
    print(f"Max distance: {max_distance} LY")
    print(f"Output: {output_file}")

    # Load data
    print("\n📥 Loading data...")
    try:
        binary_df = pd.read_csv(binary_file, sep='\t')
        faction_df = pd.read_csv(faction_file, sep='\t')
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    print(f"📊 Binary systems loaded: {len(binary_df):,}")
    print(f"📊 Faction systems loaded: {len(faction_df):,}")

    # Validate required columns
    required_cols = ['system_name', 'coords_x', 'coords_y', 'coords_z']
    for col in required_cols:
        if col not in binary_df.columns:
            print(f"❌ Error: Missing column '{col}' in binary systems file")
            sys.exit(1)
        if col not in faction_df.columns:
            print(f"❌ Error: Missing column '{col}' in faction systems file")
            sys.exit(1)

    # Setup multiprocessing
    num_processes = mp.cpu_count()
    print(f"\n⚙️  Using {num_processes} processes")

    # Split binary systems into chunks for parallel processing
    binary_chunks = split_dataframe(binary_df, num_processes)
    print(f"📦 Split binary systems into {len(binary_chunks)} chunks")

    # Extract faction coordinates (needed by all processes)
    faction_coords = faction_df[['system_name', 'coords_x', 'coords_y', 'coords_z']].copy()

    # Create partial function with fixed parameters
    process_chunk = partial(find_nearby_systems_chunk,
                           faction_coords=faction_coords,
                           max_distance=max_distance)

    print(f"\n🔄 Processing chunks in parallel...")
    start_time = time.time()

    # Process chunks in parallel
    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(process_chunk, binary_chunks)

    # Combine results
    print("🔗 Combining results...")
    nearby_systems = []
    for chunk_result in results:
        nearby_systems.extend(chunk_result)

    processing_time = time.time() - start_time

    print(f"\n✅ Processing complete!")
    print(f"⏱️  Processing time: {processing_time:.1f}s")
    print(f"📊 Binary systems within {max_distance} LY: {len(nearby_systems):,}")
    print(f"📈 Match rate: {(len(nearby_systems) / len(binary_df) * 100):.2f}%")

    if nearby_systems:
        # Convert to DataFrame and save
        result_df = pd.DataFrame(nearby_systems)

        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save results
        result_df.to_csv(output_file, sep='\t', index=False)
        print(f"💾 Results saved to: {output_file}")

        # Show sample of results
        print(f"\n📋 Sample results:")
        print(result_df[['system_name', 'closest_faction_system', 'distance_to_faction']].head(10).to_string(index=False))
    else:
        print("⚠️  No binary systems found within the specified distance")


if __name__ == "__main__":
    main()