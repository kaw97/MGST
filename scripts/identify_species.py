#!/usr/bin/env python3
"""
Script to scan enriched codex entries and identify all unique genus/species combinations.
Creates a species list file for use by the organization script.
"""

import json
import sys
from pathlib import Path
from typing import Set, Tuple
import argparse
import time

def parse_species_name(english_name: str) -> Tuple[str, str]:
    """
    Extract genus and species from english_name field.
    Examples:
    - "Bacterium Cerbrus - Indigo" -> ("Bacterium", "Cerbrus")
    - "Frutexa Flabellum - Emerald" -> ("Frutexa", "Flabellum")
    - "Osseus Spiralis - Indigo" -> ("Osseus", "Spiralis")
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

def scan_for_unique_species(input_file: Path) -> Set[Tuple[str, str]]:
    """
    Scan the enriched codex file to identify all unique genus/species combinations.
    """
    unique_species = set()
    total_entries = 0

    print(f"Scanning {input_file} for unique species...")
    print("Progress will be reported every 100,000 entries...")

    start_time = time.time()

    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 100000 == 0:
                elapsed = time.time() - start_time
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"  Scanned {line_num:,} entries, found {len(unique_species)} unique species (rate: {rate:.0f} entries/sec)")

            try:
                entry = json.loads(line.strip())
                english_name = entry.get('english_name', '')
                genus, species = parse_species_name(english_name)
                unique_species.add((genus, species))
                total_entries = line_num
            except json.JSONDecodeError:
                continue

    elapsed_time = time.time() - start_time
    print(f"\nScan complete!")
    print(f"Total entries scanned: {total_entries:,}")
    print(f"Unique genus/species combinations: {len(unique_species)}")
    print(f"Total time: {elapsed_time:.1f} seconds")
    print(f"Average rate: {total_entries/elapsed_time:.0f} entries/second")

    return unique_species

def save_species_list(unique_species: Set[Tuple[str, str]], output_file: Path):
    """
    Save the unique species list to a JSON file.
    """
    species_list = [{"genus": genus, "species": species} for genus, species in sorted(unique_species)]

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(species_list, f, indent=2)

    print(f"Species list saved to: {output_file}")

def print_species_summary(unique_species: Set[Tuple[str, str]]):
    """
    Print a summary of identified species by genus.
    """
    # Group by genus
    genus_counts = {}
    for genus, species in unique_species:
        if genus not in genus_counts:
            genus_counts[genus] = set()
        genus_counts[genus].add(species)

    print(f"\nSpecies summary by genus:")
    print(f"{'Genus':<20} {'Species Count':<15} {'Example Species'}")
    print("-" * 60)

    for genus in sorted(genus_counts.keys()):
        species_set = genus_counts[genus]
        example_species = sorted(species_set)[0] if species_set else "None"
        print(f"{genus:<20} {len(species_set):<15} {example_species}")

def main():
    parser = argparse.ArgumentParser(description='Identify unique genus/species combinations in enriched codex')
    parser.add_argument('--input-file', required=True, help='Path to enriched codex JSONL file')
    parser.add_argument('--output-file', required=True, help='Path to save species list JSON file')

    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_file = Path(args.output_file)

    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    print("=== Species Identification Phase ===")
    unique_species = scan_for_unique_species(input_file)

    print_species_summary(unique_species)

    save_species_list(unique_species, output_file)

    print(f"\nSpecies identification complete!")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    main()