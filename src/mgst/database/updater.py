"""Incremental galaxy database updater with change tracking."""

import gzip
import json
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple
import multiprocessing as mp
from collections import defaultdict

from .downloader import SpanshDownloader
from .change_detector import ChangeDetector
from .schema import (SystemChangeRecord, StationChangeRecord, TimeSeriesWriter)

logger = logging.getLogger(__name__)


@dataclass
class UpdateStats:
    """Statistics for database update operations."""
    systems_processed: int = 0
    systems_changed: int = 0
    systems_discovered: int = 0
    stations_changed: int = 0
    stations_discovered: int = 0
    change_records_written: int = 0
    update_time_seconds: float = 0.0
    dataset_type: str = ""


class SystemStateManager:
    """Manages current system states for change detection."""
    
    def __init__(self, galaxy_sectors_dir: Path):
        """Initialize system state manager.
        
        Args:
            galaxy_sectors_dir: Directory containing current sector files
        """
        self.galaxy_sectors_dir = Path(galaxy_sectors_dir)
        self._system_cache: Dict[int, Dict[str, Any]] = {}
        self._station_cache: Dict[int, Dict[str, Any]] = {}
        self._loaded_sectors: Set[str] = set()
        
    def load_sector_systems(self, sector_name: str) -> None:
        """Load systems from a sector file into cache.
        
        Args:
            sector_name: Name of sector to load (e.g., 'sector_0_0_0')
        """
        if sector_name in self._loaded_sectors:
            return
            
        sector_file = self.galaxy_sectors_dir / f"{sector_name}.jsonl.gz"
        if not sector_file.exists():
            logger.debug(f"Sector file not found: {sector_file}")
            self._loaded_sectors.add(sector_name)
            return
            
        logger.debug(f"Loading sector: {sector_name}")
        
        try:
            with gzip.open(sector_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        system = json.loads(line)
                        system_id = system.get('id64')
                        
                        if system_id:
                            self._system_cache[system_id] = system
                            
                            # Cache stations
                            for station in system.get('stations', []):
                                station_id = station.get('id')
                                if station_id:
                                    self._station_cache[station_id] = station
                                    
            self._loaded_sectors.add(sector_name)
            logger.debug(f"Loaded {len(self._system_cache)} systems from {sector_name}")
            
        except Exception as e:
            logger.error(f"Failed to load sector {sector_name}: {e}")
            self._loaded_sectors.add(sector_name)  # Mark as attempted
    
    def get_system(self, system_id: int, coords: Optional[Dict[str, float]] = None) -> Optional[Dict[str, Any]]:
        """Get current state of a system.
        
        Args:
            system_id: System ID64
            coords: System coordinates (for sector loading)
            
        Returns:
            System dictionary or None if not found
        """
        # Check cache first
        if system_id in self._system_cache:
            return self._system_cache[system_id]
            
        # Try to load relevant sector if coordinates available
        if coords:
            sector_name = self._get_sector_name(coords)
            self.load_sector_systems(sector_name)
            
            # Check cache again after loading
            if system_id in self._system_cache:
                return self._system_cache[system_id]
                
        return None
    
    def get_station(self, station_id: int, system_coords: Optional[Dict[str, float]] = None) -> Optional[Dict[str, Any]]:
        """Get current state of a station.
        
        Args:
            station_id: Station ID
            system_coords: System coordinates (for sector loading)
            
        Returns:
            Station dictionary or None if not found
        """
        # Check cache first
        if station_id in self._station_cache:
            return self._station_cache[station_id]
            
        # Try to load relevant sector if coordinates available
        if system_coords:
            sector_name = self._get_sector_name(system_coords)
            self.load_sector_systems(sector_name)
            
            # Check cache again after loading
            if station_id in self._station_cache:
                return self._station_cache[station_id]
                
        return None
    
    def update_system(self, system: Dict[str, Any]) -> None:
        """Update system in cache.
        
        Args:
            system: Updated system dictionary
        """
        system_id = system.get('id64')
        if system_id:
            self._system_cache[system_id] = system
            
            # Update stations in cache
            for station in system.get('stations', []):
                station_id = station.get('id')
                if station_id:
                    self._station_cache[station_id] = station
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics.
        
        Returns:
            Cache statistics
        """
        return {
            'systems_cached': len(self._system_cache),
            'stations_cached': len(self._station_cache),
            'sectors_loaded': len(self._loaded_sectors)
        }
    
    @staticmethod
    def _get_sector_name(coords: Dict[str, float]) -> str:
        """Convert coordinates to sector name."""
        sector_x = int(coords['x'] // 1000)
        sector_y = int(coords['y'] // 1000)
        sector_z = int(coords['z'] // 1000)
        return f"sector_{sector_x}_{sector_y}_{sector_z}"


def update_sector_worker(args: Tuple[List[Dict[str, Any]], Path, Path, str, int]) -> Dict[str, Any]:
    """Worker function for processing system updates in parallel.
    
    Args:
        args: (systems_batch, galaxy_sectors_dir, timeseries_dir, timestamp, worker_id)
        
    Returns:
        Update statistics
    """
    systems_batch, galaxy_sectors_dir, timeseries_dir, timestamp, worker_id = args
    
    try:
        start_time = time.time()
        
        # Initialize components for this worker
        state_manager = SystemStateManager(galaxy_sectors_dir)
        change_detector = ChangeDetector()
        
        # Process systems
        stats = {
            'worker_id': worker_id,
            'systems_processed': 0,
            'systems_changed': 0,
            'systems_discovered': 0,
            'stations_changed': 0,
            'stations_discovered': 0,
            'system_change_records': [],
            'station_change_records': [],
            'updated_systems': [],
            'processing_time': 0.0
        }
        
        for system in systems_batch:
            system_id = system.get('id64')
            coords = system.get('coords')
            
            if not system_id or not coords:
                continue
                
            stats['systems_processed'] += 1
            
            # Get current system state
            old_system = state_manager.get_system(system_id, coords)
            
            # Detect system-level changes
            system_changes = change_detector.detect_system_changes(old_system, system)
            
            if system_changes['has_changes']:
                stats['systems_changed'] += 1
                
                if old_system is None:
                    stats['systems_discovered'] += 1
                    
                # Create change record
                change_record = SystemChangeRecord.from_system_diff(
                    system_id, system['name'], old_system, system, timestamp
                )
                stats['system_change_records'].append(change_record)
                
                # Process station changes
                old_stations = {s.get('id'): s for s in old_system.get('stations', [])} if old_system else {}
                new_stations = {s.get('id'): s for s in system.get('stations', [])}
                
                for station_id, new_station in new_stations.items():
                    if not station_id:
                        continue
                        
                    old_station = old_stations.get(station_id)
                    
                    # Detect station changes
                    station_changes = change_detector.detect_station_changes(old_station, new_station)
                    
                    if station_changes['has_changes']:
                        stats['stations_changed'] += 1
                        
                        if old_station is None:
                            stats['stations_discovered'] += 1
                            
                        # Create station change record
                        station_change_record = StationChangeRecord.from_station_diff(
                            station_id, system_id, new_station['name'], system['name'],
                            old_station, new_station, timestamp
                        )
                        stats['station_change_records'].append(station_change_record)
                
                # Track updated system for writing back
                stats['updated_systems'].append(system)
                
        stats['processing_time'] = time.time() - start_time
        
        logger.debug(f"Worker {worker_id} processed {stats['systems_processed']} systems, "
                    f"found {stats['systems_changed']} changed")
        
        return stats
        
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}")
        return {
            'worker_id': worker_id,
            'error': str(e),
            'systems_processed': 0,
            'systems_changed': 0,
            'systems_discovered': 0,
            'stations_changed': 0,
            'stations_discovered': 0
        }


class GalaxyDatabaseUpdater:
    """Incremental galaxy database updater with change tracking."""
    
    def __init__(self, database_base_dir: Path, max_workers: Optional[int] = None,
                 batch_size: int = 10000):
        """Initialize database updater.
        
        Args:
            database_base_dir: Base directory for database files
            max_workers: Maximum worker processes (default: CPU count)
            batch_size: Systems per batch for processing
        """
        self.database_base_dir = Path(database_base_dir)
        self.max_workers = max_workers or mp.cpu_count()
        self.batch_size = batch_size
        
        # Database directories
        self.galaxy_sectors_dir = self.database_base_dir / "galaxy_sectors_compressed"
        self.timeseries_dir = self.database_base_dir / "galaxy_timeseries"
        self.downloads_dir = self.database_base_dir / "downloads"
        
        # Ensure directories exist
        for directory in [self.timeseries_dir, self.downloads_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
        if not self.galaxy_sectors_dir.exists():
            raise ValueError(f"Galaxy sectors directory not found: {self.galaxy_sectors_dir}. "
                           "Run database build first.")
        
        self.downloader = SpanshDownloader(self.downloads_dir)
        self.timeseries_writer = TimeSeriesWriter(self.timeseries_dir)
        
        logger.info(f"Initialized updater with {self.max_workers} workers, "
                   f"batch size {self.batch_size}")
    
    def update_from_spansh(self, dataset_type: str = '1day',
                          force_download: bool = False) -> UpdateStats:
        """Update database from Spansh incremental data.
        
        Args:
            dataset_type: Type of dataset ('1day', '7days', '1month')
            force_download: Force redownload even if file exists
            
        Returns:
            Update statistics
        """
        if dataset_type not in ['1day', '7days', '1month']:
            raise ValueError(f"Invalid dataset type: {dataset_type}")
            
        logger.info(f"Starting database update from {dataset_type} dataset")
        start_time = time.time()
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # Download incremental dataset
        logger.info("Downloading incremental dataset...")
        update_file = self.downloader.download_dataset(dataset_type, force_download)
        
        file_info = self.downloader.get_file_info(update_file)
        logger.info(f"Update dataset: {file_info['size_gb']:.2f} GB")
        
        # Process updates in parallel
        logger.info("Processing system updates...")
        stats = self._process_updates_parallel(update_file, timestamp)
        
        # Calculate final statistics
        update_time = time.time() - start_time
        stats.update_time_seconds = update_time
        stats.dataset_type = dataset_type
        
        logger.info(f"Update completed in {update_time:.1f}s")
        logger.info(f"Processed {stats.systems_processed:,} systems")
        logger.info(f"Found {stats.systems_changed:,} changed systems "
                   f"({stats.systems_discovered:,} new)")
        logger.info(f"Found {stats.stations_changed:,} changed stations "
                   f"({stats.stations_discovered:,} new)")
        logger.info(f"Wrote {stats.change_records_written:,} change records")
        
        # Write update metadata
        self._write_update_metadata(stats, dataset_type, update_file, timestamp)
        
        return stats
    
    def _process_updates_parallel(self, update_file: Path, timestamp: str) -> UpdateStats:
        """Process updates using parallel workers."""
        stats = UpdateStats()
        
        # Stream and batch systems
        system_batches = []
        current_batch = []
        
        logger.info("Loading and batching updated systems...")
        for system in self.downloader.stream_systems(update_file):
            current_batch.append(system)
            
            if len(current_batch) >= self.batch_size:
                system_batches.append(current_batch)
                current_batch = []
                
            stats.systems_processed += 1
            
            if len(system_batches) % 10 == 0 and len(system_batches) > 0:
                logger.info(f"Batched {len(system_batches) * self.batch_size:,} systems")
                
        # Add final batch
        if current_batch:
            system_batches.append(current_batch)
            
        logger.info(f"Created {len(system_batches)} batches for update processing")
        
        # Prepare worker arguments
        worker_args = []
        for i, batch in enumerate(system_batches):
            worker_args.append((
                batch, 
                self.galaxy_sectors_dir, 
                self.timeseries_dir, 
                timestamp, 
                i
            ))
        
        # Process batches in parallel
        logger.info(f"Processing {len(worker_args)} update batches with {self.max_workers} workers...")
        
        all_system_change_records = []
        all_station_change_records = []
        updated_systems_by_sector = defaultdict(list)
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all jobs
            future_to_args = {
                executor.submit(update_sector_worker, args): args 
                for args in worker_args
            }
            
            # Collect results
            completed = 0
            for future in as_completed(future_to_args):
                result = future.result()
                
                if 'error' not in result:
                    stats.systems_changed += result['systems_changed']
                    stats.systems_discovered += result['systems_discovered']
                    stats.stations_changed += result['stations_changed']
                    stats.stations_discovered += result['stations_discovered']
                    
                    # Collect change records
                    all_system_change_records.extend(result['system_change_records'])
                    all_station_change_records.extend(result['station_change_records'])
                    
                    # Group updated systems by sector for efficient writing
                    for system in result['updated_systems']:
                        coords = system.get('coords')
                        if coords:
                            sector_name = self._get_sector_name(coords)
                            updated_systems_by_sector[sector_name].append(system)
                    
                completed += 1
                if completed % 10 == 0:
                    logger.info(f"Completed {completed}/{len(worker_args)} update batches")
        
        # Write change records to time-series database
        if all_system_change_records or all_station_change_records:
            logger.info("Writing change records to time-series database...")
            partition = datetime.fromisoformat(timestamp.rstrip('Z')).strftime('%Y%m')
            
            if all_system_change_records:
                self.timeseries_writer.write_system_changes(all_system_change_records, partition)
                stats.change_records_written += len(all_system_change_records)
                
            if all_station_change_records:
                self.timeseries_writer.write_station_changes(all_station_change_records, partition)
                stats.change_records_written += len(all_station_change_records)
        
        # Update current sector files with changed systems
        if updated_systems_by_sector:
            logger.info("Updating current sector files...")
            self._update_sector_files(updated_systems_by_sector)
        
        logger.info(f"Parallel update processing complete")
        
        return stats
    
    def _update_sector_files(self, updated_systems_by_sector: Dict[str, List[Dict[str, Any]]]) -> None:
        """Update sector files with changed systems.
        
        Args:
            updated_systems_by_sector: Dictionary mapping sector names to updated systems
        """
        for sector_name, updated_systems in updated_systems_by_sector.items():
            sector_file = self.galaxy_sectors_dir / f"{sector_name}.jsonl.gz"
            
            # Load existing systems from sector
            existing_systems = {}
            if sector_file.exists():
                try:
                    with gzip.open(sector_file, 'rt', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                system = json.loads(line)
                                system_id = system.get('id64')
                                if system_id:
                                    existing_systems[system_id] = system
                except Exception as e:
                    logger.error(f"Failed to load existing systems from {sector_file}: {e}")
                    continue
            
            # Update with new systems
            for system in updated_systems:
                system_id = system.get('id64')
                if system_id:
                    existing_systems[system_id] = system
            
            # Write back to sector file
            try:
                sector_file.parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(sector_file, 'wt', encoding='utf-8') as f:
                    for system in existing_systems.values():
                        json.dump(system, f, separators=(',', ':'))
                        f.write('\n')
                        
                logger.debug(f"Updated sector file {sector_name} with {len(updated_systems)} systems")
                
            except Exception as e:
                logger.error(f"Failed to update sector file {sector_file}: {e}")
    
    def _write_update_metadata(self, stats: UpdateStats, dataset_type: str, 
                             source_file: Path, timestamp: str) -> None:
        """Write metadata about the update process."""
        metadata = {
            'update_time': timestamp,
            'update_duration_seconds': stats.update_time_seconds,
            'dataset_type': dataset_type,
            'source_file': str(source_file),
            'systems_processed': stats.systems_processed,
            'systems_changed': stats.systems_changed,
            'systems_discovered': stats.systems_discovered,
            'stations_changed': stats.stations_changed,
            'stations_discovered': stats.stations_discovered,
            'change_records_written': stats.change_records_written,
            'max_workers': self.max_workers,
            'batch_size': self.batch_size
        }
        
        # Append to update log
        update_log_file = self.database_base_dir / "update_log.jsonl"
        with open(update_log_file, 'a') as f:
            json.dump(metadata, f, separators=(',', ':'))
            f.write('\n')
            
        logger.info(f"Update metadata appended to: {update_log_file}")
    
    @staticmethod
    def _get_sector_name(coords: Dict[str, float]) -> str:
        """Convert coordinates to sector name."""
        sector_x = int(coords['x'] // 1000)
        sector_y = int(coords['y'] // 1000)
        sector_z = int(coords['z'] // 1000)
        return f"sector_{sector_x}_{sector_y}_{sector_z}"