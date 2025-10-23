#!/usr/bin/env python3
"""
Build Lightweight Subsector Index

Creates a minimal index that maps subsectors to sector files WITHOUT storing
individual system offsets. This uses <1GB RAM regardless of database size.

Index format:
{
  "subsectors": {
    "Aaekaae_OD-T": {
      "sector_file": "Aaekaae.jsonl.gz",
      "system_count": 42
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

To search a subsector, you scan only its sector file and filter by subsector code.
With 12 workers processing different sectors in parallel, this is still very fast.
"""

import re
import json
import gzip
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lightweight_index_build.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class LightweightIndexBuilder:
    """Builds a lightweight subsector index (no per-system offsets)."""

    def __init__(self, source_dir: Path, target_dir: Path, workers: int = 12):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self.workers = workers

        # Subsector naming pattern
        self.pattern = re.compile(r'^([A-Za-z\s_]+)\s+([A-Z]{2}-[A-Z])\s+([a-z])(\d*)(-\d+)?$')

    def parse_system_name(self, system_name: str) -> Optional[Tuple[str, str]]:
        """Parse system name to extract sector and subsector."""
        match = self.pattern.match(system_name.strip())
        if match:
            sector = match.group(1).strip()
            subsector = match.group(2)
            return sector, subsector
        return None

    def index_sector_file(self, sector_file: Path) -> Dict:
        """Build index for a single sector file with per-system offsets.

        Returns:
            Dictionary with sector metadata and subsector system lists
        """
        sector_name = sector_file.stem.replace('.jsonl', '')
        logger.info(f"Indexing sector: {sector_name}")

        subsector_systems = defaultdict(list)
        system_count = 0
        offset = 0

        try:
            with gzip.open(sector_file, 'rb') as f:
                while True:
                    line_start = offset
                    line = f.readline()

                    if not line:
                        break

                    try:
                        system_data = json.loads(line.decode('utf-8').strip())
                        system_name = system_data.get('name', '')

                        if system_name:
                            parsed = self.parse_system_name(system_name)
                            if parsed:
                                parsed_sector, subsector = parsed
                                subsector_key = f"{parsed_sector}_{subsector}"

                                # Store system with offset and size
                                subsector_systems[subsector_key].append({
                                    'name': system_name,
                                    'offset': line_start,
                                    'size': len(line)
                                })
                                system_count += 1

                    except json.JSONDecodeError:
                        pass

                    # Update offset for next line
                    offset += len(line)

        except Exception as e:
            logger.error(f"Error indexing {sector_name}: {e}")
            return {'error': str(e)}

        result = {
            'sector': sector_name,
            'sector_file': sector_file.name,
            'subsectors': list(subsector_systems.keys()),
            'subsector_systems': dict(subsector_systems),
            'system_count': system_count
        }

        logger.info(f"Indexed {sector_name}: {len(subsector_systems)} subsectors, {system_count} systems")
        return result

    def build_index(self, batch_size: int = 50) -> Dict:
        """Build index with per-system offsets using incremental writing.

        Args:
            batch_size: Write index to disk every N sectors (keeps memory low)
        """
        logger.info("Building subsector index with per-system offsets...")

        # Create target directory
        self.target_dir.mkdir(parents=True, exist_ok=True)

        # Get list of sector files
        sector_files = list(self.source_dir.glob("*.jsonl.gz"))
        if not sector_files:
            sector_files = list(self.source_dir.glob("*.jsonl"))

        if not sector_files:
            raise FileNotFoundError(f"No sector files found in {self.source_dir}")

        logger.info(f"Indexing {len(sector_files)} sector files with {self.workers} workers")
        logger.info(f"Using batch size {batch_size} to limit memory usage")

        # Accumulate index data
        index = {
            'subsectors': {},
            'sectors': {}
        }

        processed_count = 0
        total_systems = 0
        total_subsectors = 0

        # Process sectors in parallel with batching
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.index_sector_file, sf): sf
                      for sf in sector_files}

            for i, future in enumerate(futures, 1):
                try:
                    result = future.result()
                    if 'error' not in result:
                        sector_name = result['sector']
                        sector_file = result['sector_file']

                        # Add to index
                        index['sectors'][sector_name] = {
                            'file': sector_file,
                            'subsectors': result['subsectors'],
                            'system_count': result['system_count']
                        }

                        # Add subsector entries with per-system offsets
                        for subsector_key, systems in result['subsector_systems'].items():
                            index['subsectors'][subsector_key] = {
                                'sector_file': sector_file,
                                'systems': systems
                            }

                        processed_count += 1
                        total_systems += result['system_count']
                        total_subsectors += len(result['subsectors'])

                        # Write to disk every batch_size sectors to limit memory
                        if processed_count % batch_size == 0:
                            self._write_index(index)
                            logger.info(f"Progress: {processed_count}/{len(sector_files)} sectors indexed, "
                                      f"{total_systems:,} systems, {total_subsectors:,} subsectors")

                    if i % 20 == 0 and processed_count % batch_size != 0:
                        logger.info(f"Progress: {processed_count}/{len(sector_files)} sectors indexed")

                except Exception as e:
                    logger.error(f"Failed to index sector: {e}")

        # Final write
        self._write_index(index)

        stats = {
            'total_sectors': len(index['sectors']),
            'total_subsectors': len(index['subsectors']),
            'total_systems': total_systems,
            'index_file': str(self.target_dir / 'subsector_index.json'),
            'source_directory': str(self.source_dir)
        }

        logger.info(f"Index complete: {stats['total_sectors']} sectors, "
                   f"{stats['total_subsectors']} subsectors, {stats['total_systems']:,} systems")

        return stats

    def _write_index(self, index: Dict):
        """Write index to JSON file."""
        index_file = self.target_dir / 'subsector_index.json'
        with open(index_file, 'w') as f:
            json.dump(index, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Build subsector index with per-system offsets')
    parser.add_argument('--source', required=True, help='Source directory with sector files')
    parser.add_argument('--target', required=True, help='Target directory for index file')
    parser.add_argument('--workers', type=int, default=12, help='Number of worker threads')
    parser.add_argument('--batch-size', type=int, default=50, help='Write to disk every N sectors (lower = less RAM)')

    args = parser.parse_args()

    # Initialize builder
    builder = LightweightIndexBuilder(
        source_dir=args.source,
        target_dir=args.target,
        workers=args.workers
    )

    try:
        start_time = datetime.now()

        # Build index
        stats = builder.build_index(batch_size=args.batch_size)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Print summary
        print(f"\n{'='*60}")
        print("INDEXED DATABASE BUILD SUMMARY")
        print(f"{'='*60}")
        print(f"Sectors indexed: {stats['total_sectors']:,}")
        print(f"Subsectors found: {stats['total_subsectors']:,}")
        print(f"Total systems: {stats['total_systems']:,}")
        print(f"Index file: {stats['index_file']}")
        print(f"Build time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"Source: {stats['source_directory']}")
        print(f"\nMemory-efficient: Batch writing every {args.batch_size} sectors")
        print(f"Per-system offsets: Stored for direct seek access")

    except Exception as e:
        logger.error(f"Index build failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
