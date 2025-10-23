#!/usr/bin/env python3
"""
Script to organize enriched codex entries by genus and species using a pre-generated species list.
Creates directory structure and sorts all entries into appropriate files.
"""

import json
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from threading import Lock
import argparse
from typing import Dict, List, Any, Tuple
import time

# Thread-safe counters and statistics
stats_lock = Lock()
processed_entries = 0
created_directories = 0
species_counts = defaultdict(int)

def parse_species_name(english_name: str) -> Tuple[str, str]:
    """
    Extract genus and species from english_name field.
    """
    if not english_name:
        return "Unknown", "Unknown"

    # Remove the color variant part (everything after " - ")
    base_name = english_name.split(" - ")[0]

    # Split into words and take first two as genus and species
    parts = base_name.split()
    if len(parts) >= 2:
        genus = parts[0]
        species = parts[1]
        return genus, species
    elif len(parts) == 1:
        return parts[0], "Unknown"
    else:
        return "Unknown", "Unknown"

def load_species_list(species_file: Path) -> List[Tuple[str, str]]:
    """
    Load the species list from JSON file.
    """
    with open(species_file, 'r', encoding='utf-8') as f:
        species_data = json.load(f)

    species_list = [(item['genus'], item['species']) for item in species_data]
    print(f"Loaded {len(species_list)} unique species from {species_file}")

    return species_list

def create_directory_structure(analysis_dir: Path, species_list: List[Tuple[str, str]]) -> Dict[Tuple[str, str], Path]:
    """
    Create directory structure for all genus/species combinations.
    Returns mapping of (genus, species) -> file_path
    """
    global created_directories

    file_paths = {}

    print(f"Creating directory structure in {analysis_dir}...")

    for genus, species in species_list:
        # Create genus directory
        genus_dir = analysis_dir / genus
        genus_dir.mkdir(exist_ok=True)

        # Create species directory
        species_dir = genus_dir / species
        species_dir.mkdir(exist_ok=True)

        # Define output file path
        file_path = species_dir / f"{genus}_{species}_entries.jsonl"
        file_paths[(genus, species)] = file_path

        with stats_lock:
            created_directories += 1

        if created_directories % 50 == 0:
            print(f"  Created {created_directories} directories...")

    print(f"Directory structure complete: {created_directories} directories created")
    return file_paths

def process_chunk(chunk_data: List[str], file_paths: Dict[Tuple[str, str], Path], chunk_id: int) -> Dict[Tuple[str, str], int]:
    """
    Process a chunk of JSONL lines and write entries to appropriate species files.
    """
    global processed_entries
    local_stats = defaultdict(int)
    local_files = {}  # Cache for open file handles

    try:
        for line in chunk_data:
            if not line.strip():
                continue

            try:
                entry = json.loads(line.strip())
                english_name = entry.get('english_name', '')
                genus, species = parse_species_name(english_name)

                # Get file path for this species
                file_path = file_paths.get((genus, species))
                if not file_path:
                    # Skip entries for species not in our list
                    continue

                # Open file if not already cached
                if file_path not in local_files:
                    local_files[file_path] = open(file_path, 'a', encoding='utf-8')

                # Write entry to appropriate file
                local_files[file_path].write(line)
                local_stats[(genus, species)] += 1

                with stats_lock:
                    processed_entries += 1

            except json.JSONDecodeError:
                continue

    finally:
        # Close all file handles
        for f in local_files.values():
            f.close()

    return local_stats

def sort_entries_by_species(input_file: Path, file_paths: Dict[Tuple[str, str], Path], workers: int = 8, chunk_size: int = 10000):
    """
    Sort all entries in the enriched codex file into appropriate species directories using multithreading.
    """
    print(f"Sorting entries from {input_file} using {workers} workers...")
    print(f"Chunk size: {chunk_size} entries per chunk")

    # Read file in chunks and process with thread pool
    chunk_id = 0
    all_stats = defaultdict(int)
    start_time = time.time()

    with open(input_file, 'r', encoding='utf-8') as f:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []

            while True:
                chunk_lines = []
                for _ in range(chunk_size):
                    line = f.readline()
                    if not line:
                        break
                    chunk_lines.append(line)

                if not chunk_lines:
                    break

                # Submit chunk for processing
                future = executor.submit(process_chunk, chunk_lines, file_paths, chunk_id)
                futures.append(future)
                chunk_id += 1

                if chunk_id % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_entries / elapsed if elapsed > 0 else 0
                    print(f"  Submitted {chunk_id} chunks, processed {processed_entries:,} entries (rate: {rate:.0f} entries/sec)")

            print(f"All {chunk_id} chunks submitted, waiting for completion...")

            # Collect results
            completed_chunks = 0
            for future in as_completed(futures):
                try:
                    chunk_stats = future.result()
                    for key, count in chunk_stats.items():
                        all_stats[key] += count

                    completed_chunks += 1
                    if completed_chunks % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = processed_entries / elapsed if elapsed > 0 else 0
                        print(f"  Completed {completed_chunks}/{chunk_id} chunks, processed {processed_entries:,} entries (rate: {rate:.0f} entries/sec)")

                except Exception as e:
                    print(f"Error processing chunk: {e}")

    return all_stats

def main():
    parser = argparse.ArgumentParser(description='Organize enriched codex entries by genus and species')
    parser.add_argument('--input-file', required=True, help='Path to enriched codex JSONL file')
    parser.add_argument('--species-file', required=True, help='Path to species list JSON file')
    parser.add_argument('--analysis-dir', required=True, help='Path to analysis directory')
    parser.add_argument('--workers', type=int, default=8, help='Number of worker threads (default: 8)')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Chunk size for processing (default: 10000)')

    args = parser.parse_args()

    input_file = Path(args.input_file)
    species_file = Path(args.species_file)
    analysis_dir = Path(args.analysis_dir)

    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)

    if not species_file.exists():
        print(f"Error: Species file {species_file} does not exist")
        sys.exit(1)

    # Create analysis directory
    analysis_dir.mkdir(exist_ok=True)

    start_time = time.time()

    print("=== Phase 1: Loading species list ===")
    species_list = load_species_list(species_file)

    print(f"\n=== Phase 2: Creating directory structure ===")
    file_paths = create_directory_structure(analysis_dir, species_list)

    print(f"\n=== Phase 3: Sorting entries by species ===")
    stats = sort_entries_by_species(input_file, file_paths, args.workers, args.chunk_size)

    # Print final statistics
    elapsed_time = time.time() - start_time
    print(f"\n=== Organization Complete ===")
    print(f"Total time: {elapsed_time:.1f} seconds")
    print(f"Processed entries: {processed_entries:,}")
    print(f"Created directories: {created_directories}")
    print(f"Unique species with entries: {len(stats)}")
    print(f"Processing rate: {processed_entries/elapsed_time:.0f} entries/second")

    # Show top 10 most common species
    print(f"\nTop 10 most common species:")
    sorted_stats = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    for i, ((genus, species), count) in enumerate(sorted_stats[:10], 1):
        print(f"  {i:2}. {genus} {species}: {count:,} entries")

    # Show species with no entries (if any)
    species_with_entries = set(stats.keys())
    all_species = set(species_list)
    empty_species = all_species - species_with_entries

    if empty_species:
        print(f"\nSpecies with no entries: {len(empty_species)}")
        if len(empty_species) <= 10:
            for genus, species in sorted(empty_species):
                print(f"  {genus} {species}")
        else:
            print(f"  (Too many to list - {len(empty_species)} species)")

    print(f"\nAll entries have been organized into {analysis_dir}")

if __name__ == "__main__":
    main()