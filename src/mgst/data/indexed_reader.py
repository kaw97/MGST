"""
Indexed Database Reader

Efficient parallel reader for indexed sector databases.
Uses sector index to quickly locate and read specific systems.
"""

import json
import gzip
from pathlib import Path
from typing import Dict, List, Optional, Iterator, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)


class IndexedDatabaseReader:
    """Reader for indexed sector databases with efficient sector lookup."""

    def __init__(self, database_path: Path, index_file: Optional[Path] = None):
        """Initialize reader with database path and index.

        Args:
            database_path: Path to directory containing sector JSONL.gz files
            index_file: Path to sector_index.json (defaults to database_path/sector_index.json)
        """
        self.database_path = Path(database_path)

        if index_file is None:
            index_file = self.database_path / 'sector_index.json'

        self.index_file = Path(index_file)

        # Load index
        self.index = self._load_index()

    def _load_index(self) -> Dict:
        """Load sector index from file."""
        if not self.index_file.exists():
            raise FileNotFoundError(f"Index file not found: {self.index_file}")

        with open(self.index_file, 'r') as f:
            return json.load(f)

    def get_sectors(self) -> List[str]:
        """Get list of all sectors in database."""
        return list(self.index['sectors'].keys())

    def get_subsectors(self, sector: Optional[str] = None) -> List[str]:
        """Get list of subsectors (for legacy/internal use with older databases).

        Note: Current architecture uses sector-level files only. This method
        is retained for compatibility with index files that track subsector-level
        statistics within sector files.

        Args:
            sector: Optional sector name to filter subsectors

        Returns:
            List of subsector keys (e.g., "Aaekaae_OD-T")
        """
        if sector:
            sector_data = self.index['sectors'].get(sector)
            if sector_data:
                return sector_data.get('subsectors', [])
            return []

        return list(self.index.get('subsectors', {}).keys())

    def get_sector_info(self, sector: str) -> Optional[Dict]:
        """Get metadata for a specific sector."""
        return self.index['sectors'].get(sector)

    def get_subsector_info(self, subsector: str) -> Optional[Dict]:
        """Get metadata for a specific subsector (legacy/internal use)."""
        return self.index.get('subsectors', {}).get(subsector)

    def read_sector(self, sector: str) -> Iterator[Dict[str, Any]]:
        """Read all systems from a sector file.

        Args:
            sector: Sector name

        Yields:
            System data dictionaries
        """
        sector_info = self.index['sectors'].get(sector)
        if not sector_info:
            logger.warning(f"Sector not found: {sector}")
            return

        sector_file = self.database_path / sector_info['file']
        if not sector_file.exists():
            logger.error(f"Sector file not found: {sector_file}")
            return

        try:
            with gzip.open(sector_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    try:
                        system_data = json.loads(line.strip())
                        yield system_data
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error reading sector {sector}: {e}")

    def read_subsector(self, subsector: str) -> Iterator[Dict[str, Any]]:
        """Read all systems from a specific subsector.

        Args:
            subsector: Subsector key (e.g., "Aaekaae_OD-T")

        Yields:
            System data dictionaries
        """
        subsector_info = self.index['subsectors'].get(subsector)
        if not subsector_info:
            logger.warning(f"Subsector not found: {subsector}")
            return

        sector_file = self.database_path / subsector_info['sector_file']
        if not sector_file.exists():
            logger.error(f"Sector file not found: {sector_file}")
            return

        try:
            with gzip.open(sector_file, 'rb') as f:
                for system_entry in subsector_info['systems']:
                    try:
                        # Seek to system offset
                        f.seek(system_entry['offset'])

                        # Read system line
                        line = f.read(system_entry['size']).decode('utf-8')
                        system_data = json.loads(line.strip())
                        yield system_data

                    except Exception as e:
                        logger.debug(f"Error reading system {system_entry['name']}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error reading subsector {subsector}: {e}")

    def read_subsectors_parallel(self, subsectors: List[str], workers: int = 4) -> Iterator[Dict[str, Any]]:
        """Read multiple subsectors in parallel.

        Args:
            subsectors: List of subsector keys
            workers: Number of parallel workers

        Yields:
            System data dictionaries from all subsectors
        """
        def read_subsector_list(subsector: str) -> List[Dict[str, Any]]:
            """Helper to read subsector into a list."""
            return list(self.read_subsector(subsector))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(read_subsector_list, ss): ss for ss in subsectors}

            for future in as_completed(futures):
                try:
                    systems = future.result()
                    for system in systems:
                        yield system
                except Exception as e:
                    subsector = futures[future]
                    logger.error(f"Error reading subsector {subsector}: {e}")

    def read_sectors_parallel(self, sectors: List[str], workers: int = 4) -> Iterator[Dict[str, Any]]:
        """Read multiple sectors in parallel.

        Args:
            sectors: List of sector names
            workers: Number of parallel workers

        Yields:
            System data dictionaries from all sectors
        """
        def read_sector_list(sector: str) -> List[Dict[str, Any]]:
            """Helper to read sector into a list."""
            return list(self.read_sector(sector))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(read_sector_list, s): s for s in sectors}

            for future in as_completed(futures):
                try:
                    systems = future.result()
                    for system in systems:
                        yield system
                except Exception as e:
                    sector = futures[future]
                    logger.error(f"Error reading sector {sector}: {e}")

    def get_statistics(self) -> Dict:
        """Get database statistics."""
        return {
            'total_sectors': len(self.index['sectors']),
            'total_subsectors': len(self.index['subsectors']),
            'total_systems': sum(s['system_count'] for s in self.index['sectors'].values()),
            'database_path': str(self.database_path),
            'index_file': str(self.index_file)
        }
