#!/usr/bin/env python3
"""
Build Indexed Sector Database

Creates an indexed database where:
- Data is stored in sector-level JSONL.gz files (one file per sector)
- A separate index file maps subsectors to byte offsets within sector files
- Enables efficient parallel searching by subsector

Index format:
{
  "subsectors": {
    "Aaekaae_OD-T": {
      "sector_file": "Aaekaae.jsonl.gz",
      "systems": [
        {"name": "Aaekaae OD-T d3-0", "offset": 12345, "size": 678},
        ...
      ]
    }
  },
  "sectors": {
    "Aaekaae": {
      "file": "Aaekaae.jsonl.gz",
      "subsectors": ["OD-T", "IR-W", ...],
      "system_count": 1234
    }
  }
}
"""

import re
import json
import gzip
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indexed_database_build.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class IndexedDatabaseBuilder:
    """Builds an indexed sector database with subsector lookup capability."""

    def __init__(self, source_dir: Path, target_dir: Path, workers: int = 4):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.workers = workers

        # Subsector naming pattern
        self.pattern = re.compile(r'^([A-Za-z\s_]+)\s+([A-Z]{2}-[A-Z])\s+([a-z])(\d*)(-\d+)?$')

        # Index structure
        self.index = {
            'subsectors': defaultdict(lambda: {'sector_file': '', 'systems': []}),
            'sectors': defaultdict(lambda: {'file': '', 'subsectors': set(), 'system_count': 0})
        }

    def parse_system_name(self, system_name: str) -> Optional[Tuple[str, str]]:
        """Parse system name to extract sector and subsector."""
        match = self.pattern.match(system_name.strip())
        if match:
            sector = match.group(1).strip()
            subsector = match.group(2)
            return sector, subsector
        return None

    def index_sector_file(self, sector_file: Path) -> Dict:
        """Build index for a single sector file.

        Returns:
            Dictionary with sector metadata and subsector index
        """
        sector_name = sector_file.stem.replace('.jsonl', '')
        logger.info(f"Indexing sector: {sector_name}")

        subsector_index = defaultdict(list)
        system_count = 0
        offset = 0

        try:
            with gzip.open(sector_file, 'rt', encoding='utf-8') as f:
                while True:
                    line_start = offset
                    line = f.readline()

                    if not line:
                        break

                    try:
                        system_data = json.loads(line.strip())
                        system_name = system_data.get('name', '')

                        if system_name:
                            parsed = self.parse_system_name(system_name)
                            if parsed:
                                parsed_sector, subsector = parsed

                                # Record system location in index
                                subsector_key = f"{parsed_sector}_{subsector}"
                                subsector_index[subsector_key].append({
                                    'name': system_name,
                                    'offset': line_start,
                                    'size': len(line.encode('utf-8'))
                                })
                                system_count += 1

                    except json.JSONDecodeError:
                        pass

                    # Update offset for next line
                    offset += len(line.encode('utf-8'))

        except Exception as e:
            logger.error(f"Error indexing {sector_name}: {e}")
            return {'error': str(e)}

        result = {
            'sector': sector_name,
            'subsectors': list(subsector_index.keys()),
            'subsector_index': dict(subsector_index),
            'system_count': system_count
        }

        logger.info(f"Indexed {sector_name}: {len(subsector_index)} subsectors, {system_count} systems")
        return result

    def build_index(self) -> Dict:
        """Build complete index for all sector files."""
        logger.info("Building database index...")

        # Create target directory
        self.target_dir.mkdir(parents=True, exist_ok=True)

        # Get list of sector files
        sector_files = list(self.source_dir.glob("*.jsonl.gz"))
        if not sector_files:
            sector_files = list(self.source_dir.glob("*.jsonl"))

        if not sector_files:
            raise FileNotFoundError(f"No sector files found in {self.source_dir}")

        logger.info(f"Indexing {len(sector_files)} sector files with {self.workers} workers")

        # Process sectors in parallel
        successful_indexes = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.index_sector_file, sf): sf
                      for sf in sector_files}

            for i, future in enumerate(futures, 1):
                try:
                    result = future.result()
                    if 'error' not in result:
                        successful_indexes.append(result)

                        # Update main index
                        sector_name = result['sector']
                        sector_file = futures[future].name

                        self.index['sectors'][sector_name] = {
                            'file': sector_file,
                            'subsectors': result['subsectors'],
                            'system_count': result['system_count']
                        }

                        # Add subsector entries
                        for subsector_key, systems in result['subsector_index'].items():
                            self.index['subsectors'][subsector_key] = {
                                'sector_file': sector_file,
                                'systems': systems
                            }

                    if i % 100 == 0:
                        logger.info(f"Progress: {i}/{len(sector_files)} sectors indexed")

                except Exception as e:
                    logger.error(f"Failed to index sector: {e}")

        # Convert sets to lists for JSON serialization
        for sector_data in self.index['sectors'].values():
            if isinstance(sector_data['subsectors'], set):
                sector_data['subsectors'] = list(sector_data['subsectors'])

        # Save index to file
        index_file = self.target_dir / 'subsector_index.json'
        with open(index_file, 'w') as f:
            json.dump(dict(self.index), f, indent=2)

        logger.info(f"Index saved to {index_file}")

        # Generate statistics
        stats = {
            'total_sectors': len(self.index['sectors']),
            'total_subsectors': len(self.index['subsectors']),
            'total_systems': sum(s['system_count'] for s in self.index['sectors'].values()),
            'index_file': str(index_file),
            'source_directory': str(self.source_dir)
        }

        return stats


def main():
    parser = argparse.ArgumentParser(description='Build indexed sector database')
    parser.add_argument('--source', required=True, help='Source directory with sector files')
    parser.add_argument('--target', required=True, help='Target directory for index file')
    parser.add_argument('--workers', type=int, default=12, help='Number of worker threads')

    args = parser.parse_args()

    # Initialize builder
    builder = IndexedDatabaseBuilder(
        source_dir=args.source,
        target_dir=args.target,
        workers=args.workers
    )

    try:
        start_time = datetime.now()

        # Build index
        stats = builder.build_index()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Print summary
        print(f"\n{'='*60}")
        print("INDEX BUILD SUMMARY")
        print(f"{'='*60}")
        print(f"Sectors indexed: {stats['total_sectors']}")
        print(f"Subsectors found: {stats['total_subsectors']}")
        print(f"Total systems: {stats['total_systems']}")
        print(f"Index file: {stats['index_file']}")
        print(f"Build time: {duration:.1f} seconds")
        print(f"Source: {stats['source_directory']}")

    except Exception as e:
        logger.error(f"Index build failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
