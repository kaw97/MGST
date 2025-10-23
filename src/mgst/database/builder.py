"""High-performance multiprocessor galaxy database builder."""

import gzip
import json
import logging
import time
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Iterator, Optional, Tuple
import multiprocessing as mp
from collections import defaultdict
import hashlib

from .downloader import SpanshDownloader
from .schema import TimeSeriesWriter

logger = logging.getLogger(__name__)


def parse_system_name(system_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse system name to extract sector and mass code."""
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    match = re.search(mass_code_pattern, system_name)
    
    if match:
        mass_code = match.group(1)
        mass_code_start = match.start()
        sector_name = system_name[:mass_code_start].strip()
        return sector_name, mass_code
    
    return None, None


def calculate_distance(coord1: Tuple[float, float, float], coord2: Tuple[float, float, float]) -> float:
    """Calculate 3D Euclidean distance between two coordinates."""
    return ((coord1[0] - coord2[0])**2 + (coord1[1] - coord2[1])**2 + (coord1[2] - coord2[2])**2)**0.5


def find_nearest_sector(system_coords: Tuple[float, float, float], 
                       sector_centers: Dict[str, Tuple[float, float, float]]) -> str:
    """Find the nearest sector to a system's coordinates."""
    if not sector_centers:
        return "Unknown"
    
    min_distance = float('inf')
    nearest_sector = None
    
    for sector_name, center_coords in sector_centers.items():
        distance = calculate_distance(system_coords, center_coords)
        if distance < min_distance:
            min_distance = distance
            nearest_sector = sector_name
    
    return nearest_sector or "Unknown"


def _flush_sector_batch_worker(output_dir: Path, sector_name: str, lines: List[str]) -> None:
    """Helper function for workers to flush sector batches."""
    filename = sanitize_filename(sector_name) + '.jsonl.gz'
    file_path = output_dir / filename
    
    with gzip.open(file_path, 'at', encoding='utf-8') as f:
        for line in lines:
            f.write(line)


def sanitize_filename(sector_name: str) -> str:
    """Convert sector name to safe filename."""
    safe_chars = []
    for char in sector_name:
        if char.isalnum() or char in [' ', '-', '_']:
            safe_chars.append(char)
        else:
            safe_chars.append('_')
    
    # Replace spaces with underscores and collapse multiple underscores
    safe_name = ''.join(safe_chars).replace(' ', '_')
    while '__' in safe_name:
        safe_name = safe_name.replace('__', '_')
    
    return safe_name.strip('_')


@dataclass
class BuildStats:
    """Statistics for database build operations."""
    total_systems: int = 0
    total_stations: int = 0
    sectors_created: int = 0
    build_time_seconds: float = 0.0
    compressed_size_mb: float = 0.0
    original_size_mb: float = 0.0
    compression_ratio: float = 0.0


class SectorProcessor:
    """Processes systems for a specific sector range."""
    
    def __init__(self, sector_bounds: Tuple[int, int, int, int, int, int]):
        """Initialize sector processor.
        
        Args:
            sector_bounds: (min_x, max_x, min_y, max_y, min_z, max_z) in 1000 LY units
        """
        self.sector_bounds = sector_bounds
        self.systems_processed = 0
        self.stations_processed = 0
        
    @staticmethod
    def get_sector_name(coords: Dict[str, float]) -> str:
        """Convert coordinates to sector name."""
        sector_x = int(coords['x'] // 1000)
        sector_y = int(coords['y'] // 1000)
        sector_z = int(coords['z'] // 1000)
        return f"sector_{sector_x}_{sector_y}_{sector_z}"
    
    @staticmethod
    def get_coordinate_sector_name(coords: Dict[str, float]) -> str:
        """Convert coordinates to coordinate-based sector name (always works)."""
        sector_x = int(coords['x'] // 1000)
        sector_y = int(coords['y'] // 1000)
        sector_z = int(coords['z'] // 1000)
        return f"sector_{sector_x:+04d}_{sector_y:+04d}_{sector_z:+04d}"
    
    def system_in_bounds(self, coords: Dict[str, float]) -> bool:
        """Check if system coordinates are within sector bounds."""
        sector_x = int(coords['x'] // 1000)
        sector_y = int(coords['y'] // 1000)
        sector_z = int(coords['z'] // 1000)
        
        min_x, max_x, min_y, max_y, min_z, max_z = self.sector_bounds
        
        return (min_x <= sector_x <= max_x and
                min_y <= sector_y <= max_y and
                min_z <= sector_z <= max_z)
    
    def process_systems_pass1(self, systems: List[Dict[str, Any]], 
                             output_dir: Path) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
        """Pass 1: Process standard systems and collect statistics for sector centers.
        
        Args:
            systems: List of system dictionaries
            output_dir: Base output directory
            
        Returns:
            Tuple of (processing_stats, sector_stats, non_standard_systems)
        """
        # Use batched writing approach for standard systems
        sector_batches = defaultdict(list)
        sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
        non_standard_systems = []
        total_stations = 0
        batch_size = 1000  # Write every 1000 systems to manage memory
        
        # Pass 1: Separate standard (name-based) and non-standard systems
        for i, system in enumerate(systems):
            system_name = system.get('name', '')
            coords = system.get('coords', {})
            
            # Count stations
            total_stations += len(system.get('stations', []))
            
            # Parse sector from system name
            sector_name, mass_code = parse_system_name(system_name)
            
            if sector_name:
                # Standard system - add to batch and collect statistics
                system_line = json.dumps(system, separators=(',', ':')) + '\n'
                sector_batches[sector_name].append(system_line)
                
                # Track statistics for sector center calculation
                if coords:
                    stats = sector_stats[sector_name]
                    stats['count'] += 1
                    stats['sum_x'] += coords.get('x', 0)
                    stats['sum_y'] += coords.get('y', 0)
                    stats['sum_z'] += coords.get('z', 0)
            else:
                # Non-standard system - collect for Pass 2
                non_standard_systems.append(system)
            
            # Batch write every 1000 systems to manage memory
            if (i + 1) % batch_size == 0 and sector_batches:
                self._batch_write_sector_files(output_dir, sector_batches)
                sector_batches.clear()
        
        # Write final batch of standard systems
        if sector_batches:
            self._batch_write_sector_files(output_dir, sector_batches)
            
        # Get sector names from batches that were written
        written_sectors = list(sector_batches.keys()) if sector_batches else []
        
        return {
            'systems_processed': len(systems),
            'stations_processed': total_stations,
            'standard_systems': len(systems) - len(non_standard_systems),
            'non_standard_systems': len(non_standard_systems),
            'sectors_written': len(written_sectors),
            'sector_names': written_sectors
        }, dict(sector_stats), non_standard_systems

    def process_systems_pass2(self, non_standard_systems: List[Dict[str, Any]], 
                             sector_centers: Dict[str, Tuple[float, float, float]],
                             output_dir: Path) -> Dict[str, Any]:
        """Pass 2: Assign non-standard systems to nearest sectors.
        
        Args:
            non_standard_systems: List of non-standard system dictionaries
            sector_centers: Dictionary mapping sector names to center coordinates
            output_dir: Base output directory
            
        Returns:
            Processing statistics
        """
        if not non_standard_systems:
            return {'non_standard_assigned': 0, 'non_standard_skipped': 0}
            
        non_standard_assigned = 0
        non_standard_skipped = 0
        assignment_batches = defaultdict(list)
        batch_size = 1000  # Batch size for writing
        
        # Assign each non-standard system to nearest sector
        for i, system in enumerate(non_standard_systems):
            coords = system.get('coords', {})
            
            if coords and sector_centers:
                # Find nearest valid sector
                system_coords = (coords.get('x', 0), coords.get('y', 0), coords.get('z', 0))
                nearest_sector = find_nearest_sector(system_coords, sector_centers)
                
                if nearest_sector != "Unknown":
                    system_line = json.dumps(system, separators=(',', ':')) + '\n'
                    assignment_batches[nearest_sector].append(system_line)
                    non_standard_assigned += 1
                else:
                    non_standard_skipped += 1
            else:
                non_standard_skipped += 1
            
            # Batch write every 1000 systems
            if (i + 1) % batch_size == 0 and assignment_batches:
                self._batch_write_sector_files(output_dir, assignment_batches)
                assignment_batches.clear()
        
        # Write final batch
        if assignment_batches:
            self._batch_write_sector_files(output_dir, assignment_batches)
            
        return {
            'non_standard_assigned': non_standard_assigned,
            'non_standard_skipped': non_standard_skipped
        }

    def process_systems(self, systems: List[Dict[str, Any]], 
                       output_dir: Path, 
                       sector_centers: Optional[Dict[str, Tuple[float, float, float]]] = None) -> Dict[str, Any]:
        """Process systems using full 2-pass approach.
        
        Args:
            systems: List of system dictionaries
            output_dir: Base output directory  
            sector_centers: Pre-calculated sector centers (for workers in pass 2)
            
        Returns:
            Processing statistics
        """
        if sector_centers is not None:
            # This is Pass 2 - we have pre-calculated sector centers
            # Extract non-standard systems from the batch
            non_standard_systems = []
            for system in systems:
                system_name = system.get('name', '')
                sector_name, _ = parse_system_name(system_name)
                if not sector_name:
                    non_standard_systems.append(system)
            
            return self.process_systems_pass2(non_standard_systems, sector_centers, output_dir)
        else:
            # This is Pass 1 - collect statistics and process standard systems
            stats, sector_stats, non_standard_systems = self.process_systems_pass1(systems, output_dir)
            
            # Include sector statistics in return for center calculation
            stats['sector_statistics'] = sector_stats
            stats['non_standard_systems_collected'] = non_standard_systems
            
            return stats
    
    def _write_sector_file(self, sector_name: str, systems: List[Dict[str, Any]], 
                          output_dir: Path) -> None:
        """Write systems to compressed sector file."""
        sector_file = output_dir / f"{sanitize_filename(sector_name)}.jsonl.gz"
        sector_file.parent.mkdir(parents=True, exist_ok=True)
        
        with gzip.open(sector_file, 'wt', encoding='utf-8') as f:
            for system in systems:
                json.dump(system, f, separators=(',', ':'))
                f.write('\n')
    
    def _batch_write_sector_files(self, output_dir: Path, sector_batches: Dict[str, List[str]]) -> None:
        """Batch write all sector files at once to minimize I/O."""
        for sector_name, lines in sector_batches.items():
            filename = sanitize_filename(sector_name) + '.jsonl.gz'
            file_path = output_dir / filename
            
            # Append to compressed file
            with gzip.open(file_path, 'at', encoding='utf-8') as f:
                for line in lines:
                    f.write(line)
    
    def _append_to_sector_file(self, output_dir: Path, sector_name: str, line: str) -> None:
        """Append a single line to a sector file."""
        filename = sanitize_filename(sector_name) + '.jsonl.gz'
        file_path = output_dir / filename
        
        with gzip.open(file_path, 'at', encoding='utf-8') as f:
            f.write(line)


def process_sector_worker(args: Tuple[List[Dict[str, Any]], 
                                    Optional[Tuple[int, int, int, int, int, int]], 
                                    Path, int, 
                                    Optional[Dict[str, Tuple[float, float, float]]]]) -> Dict[str, Any]:
    """Worker function for processing sectors in parallel with 2-pass support.
    
    Args:
        args: (systems_batch, sector_bounds, output_dir, worker_id, sector_centers)
        sector_bounds: Sector bounds for coordinate filtering (can be None)
        sector_centers: Pre-calculated sector centers for Pass 2 (None for Pass 1)
        
    Returns:
        Processing statistics
    """
    systems_batch, sector_bounds, output_dir, worker_id, sector_centers = args
    
    # Use dummy bounds if None - coordinate sectoring processes all systems
    if sector_bounds is None:
        sector_bounds = (-999, 999, -999, 999, -999, 999)
    
    processor = SectorProcessor(sector_bounds)
    
    try:
        start_time = time.time()
        stats = processor.process_systems(systems_batch, output_dir, sector_centers)
        
        stats['worker_id'] = worker_id
        stats['processing_time'] = time.time() - start_time
        
        return stats
        
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}")
        return {
            'worker_id': worker_id,
            'error': str(e),
            'systems_processed': 0,
            'stations_processed': 0,
            'sectors_written': 0
        }


class GalaxyDatabaseBuilder:
    """High-performance galaxy database builder with multiprocessing."""
    
    def __init__(self, output_base_dir: Path, max_workers: Optional[int] = None,
                 batch_size: int = 50000):
        """Initialize database builder.
        
        Args:
            output_base_dir: Base directory for database files
            max_workers: Maximum worker processes (default: CPU count)
            batch_size: Systems per batch for processing
        """
        self.output_base_dir = Path(output_base_dir)
        self.max_workers = max_workers or mp.cpu_count()
        self.batch_size = batch_size
        
        # Create directory structure
        self.galaxy_sectors_dir = self.output_base_dir / "galaxy_sectors_compressed"
        self.timeseries_dir = self.output_base_dir / "galaxy_timeseries"
        
        # Use shared downloads directory to avoid redownloading
        shared_downloads_dir = Path("Databases") / "downloads"
        shared_downloads_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir = shared_downloads_dir
        
        for directory in [self.galaxy_sectors_dir, self.timeseries_dir, self.downloads_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
        self.downloader = SpanshDownloader(self.downloads_dir)
        self.timeseries_writer = TimeSeriesWriter(self.timeseries_dir)
        
        logger.info(f"Initialized builder with {self.max_workers} workers, "
                   f"batch size {self.batch_size}")
    
    def build_from_scratch(self, dataset_type: str = 'full',
                          force_download: bool = False) -> BuildStats:
        """Build galaxy database from scratch.
        
        Args:
            dataset_type: Type of dataset to download ('full', '1month', etc.)
            force_download: Force redownload even if file exists
            
        Returns:
            Build statistics
        """
        logger.info(f"Starting full galaxy database build from {dataset_type} dataset")
        start_time = time.time()
        
        # Download dataset
        logger.info("Downloading dataset...")
        galaxy_file = self.downloader.download_dataset(dataset_type, force_download)
        
        file_info = self.downloader.get_file_info(galaxy_file)
        logger.info(f"Dataset info: {file_info['size_gb']:.1f} GB, "
                   f"estimated {file_info.get('estimated_system_count', 'unknown')} systems")
        
        # Process systems in parallel
        logger.info("Processing systems...")
        stats = self._process_systems_parallel(galaxy_file)
        
        # Calculate final statistics
        build_time = time.time() - start_time
        
        stats.build_time_seconds = build_time
        stats.original_size_mb = file_info['size_bytes'] / (1024**2)
        
        # Calculate compressed database size
        compressed_size = sum(f.stat().st_size for f in self.galaxy_sectors_dir.rglob('*.gz'))
        stats.compressed_size_mb = compressed_size / (1024**2)
        stats.compression_ratio = (1 - stats.compressed_size_mb / stats.original_size_mb) * 100
        
        logger.info(f"Build completed in {build_time:.1f}s")
        logger.info(f"Processed {stats.total_systems:,} systems, {stats.total_stations:,} stations")
        logger.info(f"Created {stats.sectors_created} sector files")
        logger.info(f"Compression: {stats.original_size_mb:.1f} MB → "
                   f"{stats.compressed_size_mb:.1f} MB ({stats.compression_ratio:.1f}% savings)")
        
        # Write build metadata
        self._write_build_metadata(stats, dataset_type, galaxy_file)
        
        return stats
    
    def _process_systems_parallel(self, galaxy_file: Path) -> BuildStats:
        """Process systems using optimized single-pass streaming with 2-pass approach."""
        stats = BuildStats()
        
        logger.info("🚀 Starting optimized single-pass streaming with 2-pass approach...")
        
        # PASS 1: Stream once, write standard systems directly, collect non-standard
        logger.info("⚡ Pass 1: Streaming → standard systems → sector files (+ collect non-standard)...")
        pass1_results = self._execute_streaming_pass1(galaxy_file, stats)
        
        logger.info(f"✅ Pass 1: {pass1_results['standard_processed']:,} standard → sector files")
        logger.info(f"   📊 Non-standard: {pass1_results['non_standard_count']:,} → temp file")
        
        # PASS 2: Process non-standard systems if any
        if pass1_results['non_standard_count'] > 0:
            logger.info("📐 Calculating sector centers...")
            sector_centers = self._calculate_sector_centers(pass1_results['sector_stats'])
            
            logger.info(f"🎯 Pass 2: Assigning {pass1_results['non_standard_count']:,} → nearest sectors...")
            pass2_results = self._execute_streaming_pass2(pass1_results, sector_centers)
            
            logger.info(f"✅ Pass 2: {pass2_results['assigned']:,} assigned → sector files")
        
        # Update statistics
        stats.sectors_created = len(pass1_results.get('sector_stats', {}))
        
        logger.info("🎉 Optimized single-pass streaming complete!")
        return stats
    
    
    
    def _calculate_sector_centers(self, sector_stats: Dict[str, Dict[str, float]]) -> Dict[str, Tuple[float, float, float]]:
        """Calculate sector centers from Pass 1 statistics."""
        min_systems_per_sector = 10
        sector_centers = {}
        
        for sector_name, stats_data in sector_stats.items():
            count = stats_data['count']
            if count >= min_systems_per_sector:
                # Calculate center from running sums
                center_x = stats_data['sum_x'] / count
                center_y = stats_data['sum_y'] / count
                center_z = stats_data['sum_z'] / count
                
                sector_centers[sector_name] = (center_x, center_y, center_z)
        
        return sector_centers
    
    
    def _execute_streaming_pass1(self, galaxy_file: Path, stats: BuildStats) -> Dict[str, Any]:
        """Pass 1: Stream once, write standard systems directly, collect non-standard."""
        all_sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
        
        # Batch writes to reduce I/O
        sector_write_batches = defaultdict(list)
        write_batch_size = 1000
        
        # Write non-standard systems to temp file instead of keeping in memory
        import tempfile
        non_standard_temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.jsonl')
        non_standard_count = 0
        
        standard_processed = 0
        systems_processed = 0
        last_log_time = time.time()
        last_batch_write = time.time()
        
        for system in self.downloader.stream_systems(galaxy_file):
            systems_processed += 1
            stats.total_systems += 1
            stats.total_stations += len(system.get('stations', []))
            
            # Parse system name to determine if standard or non-standard
            system_name = system.get('name', '')
            coords = system.get('coords', {})
            sector_name, _ = parse_system_name(system_name)
            
            if sector_name:
                # Standard system - add to batch
                system_line = json.dumps(system, separators=(',', ':')) + '\n'
                sector_write_batches[sector_name].append(system_line)
                standard_processed += 1
                
                # Collect statistics for sector centers
                if coords:
                    stats_data = all_sector_stats[sector_name]
                    stats_data['count'] += 1
                    stats_data['sum_x'] += coords.get('x', 0)
                    stats_data['sum_y'] += coords.get('y', 0)
                    stats_data['sum_z'] += coords.get('z', 0)
                
                # Batch write when sector reaches threshold
                if len(sector_write_batches[sector_name]) >= write_batch_size:
                    self._flush_sector_batch(sector_name, sector_write_batches[sector_name])
                    sector_write_batches[sector_name].clear()
            else:
                # Non-standard system - write to temp file to save memory
                json.dump(system, non_standard_temp_file, separators=(',', ':'))
                non_standard_temp_file.write('\n')
                non_standard_count += 1
            
            # Periodic batch flushes to prevent memory buildup
            current_time = time.time()
            if current_time - last_batch_write > 60:  # Flush every minute
                self._flush_all_sector_batches(sector_write_batches)
                last_batch_write = current_time
            
            # Log progress every 30 seconds
            if current_time - last_log_time > 30:
                logger.info(f"   💫 Processed: {systems_processed:,} systems ({standard_processed:,} standard, {non_standard_count:,} non-standard)")
                last_log_time = current_time
        
        # Final flush of all remaining batches
        self._flush_all_sector_batches(sector_write_batches)
        non_standard_temp_file.close()
        
        return {
            'systems_processed': systems_processed,
            'standard_processed': standard_processed,
            'non_standard_temp_file': non_standard_temp_file.name,
            'non_standard_count': non_standard_count,
            'sector_stats': dict(all_sector_stats)
        }
    
    def _flush_sector_batch(self, sector_name: str, lines: List[str]) -> None:
        """Flush a single sector's batch to file."""
        filename = sanitize_filename(sector_name) + '.jsonl.gz'
        file_path = self.galaxy_sectors_dir / filename
        
        with gzip.open(file_path, 'at', encoding='utf-8') as f:
            for line in lines:
                f.write(line)
    
    def _flush_all_sector_batches(self, sector_batches: Dict[str, List[str]]) -> None:
        """Flush all sector batches to files."""
        for sector_name, lines in sector_batches.items():
            if lines:  # Only flush non-empty batches
                self._flush_sector_batch(sector_name, lines)
        
        # Clear all batches after writing
        for batch_list in sector_batches.values():
            batch_list.clear()
    
    def _execute_streaming_pass2(self, pass1_result: Dict[str, Any], 
                                sector_centers: Dict[str, Tuple[float, float, float]]) -> Dict[str, Any]:
        """Pass 2: Stream non-standard systems from temp file and assign to nearest sectors."""
        
        temp_file_path = pass1_result['non_standard_temp_file']
        non_standard_count = pass1_result['non_standard_count']
        
        if non_standard_count == 0:
            return {'assigned': 0, 'skipped': 0}
        
        logger.info(f"   📂 Reading {non_standard_count:,} non-standard systems from temp file...")
        
        # Batch writes for Pass 2 as well
        sector_assignment_batches = defaultdict(list)
        assignment_batch_size = 1000
        
        assigned = 0
        skipped = 0
        
        # Stream from temp file
        with open(temp_file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    system = json.loads(line.strip())
                    coords = system.get('coords', {})
                    
                    if coords:
                        # Find nearest sector
                        system_coords = (coords.get('x', 0), coords.get('y', 0), coords.get('z', 0))
                        nearest_sector = find_nearest_sector(system_coords, sector_centers)
                        
                        if nearest_sector != "Unknown":
                            # Add to assignment batch
                            system_line = json.dumps(system, separators=(',', ':')) + '\n'
                            sector_assignment_batches[nearest_sector].append(system_line)
                            assigned += 1
                            
                            # Flush batch if it gets too large
                            if len(sector_assignment_batches[nearest_sector]) >= assignment_batch_size:
                                self._flush_sector_batch(nearest_sector, sector_assignment_batches[nearest_sector])
                                sector_assignment_batches[nearest_sector].clear()
                        else:
                            skipped += 1
                    else:
                        skipped += 1
                        
                    # Progress logging
                    if line_num % 10000 == 0:
                        logger.info(f"   🎯 Pass 2 progress: {line_num:,}/{non_standard_count:,} systems ({assigned:,} assigned)")
                        
                except json.JSONDecodeError:
                    skipped += 1
                    continue
        
        # Final flush of assignment batches
        for sector_name, lines in sector_assignment_batches.items():
            if lines:
                self._flush_sector_batch(sector_name, lines)
        
        # Clean up temp file
        import os
        try:
            os.unlink(temp_file_path)
        except OSError:
            pass
        
        return {'assigned': assigned, 'skipped': skipped}
    
    @staticmethod
    def _process_chunk_pass1_worker(args: Tuple[Path, Path]) -> Dict[str, Any]:
        """Worker function for Pass 1: Process a chunk file for standard systems."""
        chunk_file, output_dir = args
        
        sector_stats = defaultdict(lambda: {'count': 0, 'sum_x': 0.0, 'sum_y': 0.0, 'sum_z': 0.0})
        sector_batches = defaultdict(list)
        
        # Create temp file for non-standard systems from this chunk
        import tempfile
        non_standard_temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl')
        
        systems_processed = 0
        standard_processed = 0
        non_standard_count = 0
        
        # Process each system in the chunk
        with open(chunk_file, 'r') as f:
            for line in f:
                try:
                    system = json.loads(line.strip())
                    systems_processed += 1
                    
                    # Parse system name
                    system_name = system.get('name', '')
                    coords = system.get('coords', {})
                    sector_name, _ = parse_system_name(system_name)
                    
                    if sector_name:
                        # Standard system - batch for writing
                        system_line = json.dumps(system, separators=(',', ':')) + '\n'
                        sector_batches[sector_name].append(system_line)
                        standard_processed += 1
                        
                        # Collect statistics
                        if coords:
                            stats_data = sector_stats[sector_name]
                            stats_data['count'] += 1
                            stats_data['sum_x'] += coords.get('x', 0)
                            stats_data['sum_y'] += coords.get('y', 0)
                            stats_data['sum_z'] += coords.get('z', 0)
                        
                        # Flush batch if it gets large
                        if len(sector_batches[sector_name]) >= 1000:
                            _flush_sector_batch_worker(output_dir, sector_name, sector_batches[sector_name])
                            sector_batches[sector_name].clear()
                    else:
                        # Non-standard system - write to temp file
                        json.dump(system, non_standard_temp, separators=(',', ':'))
                        non_standard_temp.write('\n')
                        non_standard_count += 1
                        
                except json.JSONDecodeError:
                    continue
        
        # Final flush of remaining batches
        for sector_name, lines in sector_batches.items():
            if lines:
                _flush_sector_batch_worker(output_dir, sector_name, lines)
        
        non_standard_temp.close()
        
        return {
            'systems_processed': systems_processed,
            'standard_processed': standard_processed,
            'non_standard_count': non_standard_count,
            'non_standard_file': non_standard_temp.name if non_standard_count > 0 else None,
            'sector_stats': dict(sector_stats)
        }
    
    
    @staticmethod
    def _process_chunk_pass2_worker(args: Tuple[Path, Path, Dict[str, Tuple[float, float, float]]]) -> Dict[str, Any]:
        """Worker function for Pass 2: Process non-standard systems file."""
        ns_file, output_dir, sector_centers = args
        
        assignment_batches = defaultdict(list)
        assigned = 0
        skipped = 0
        
        with open(ns_file, 'r') as f:
            for line in f:
                try:
                    system = json.loads(line.strip())
                    coords = system.get('coords', {})
                    
                    if coords:
                        # Find nearest sector
                        system_coords = (coords.get('x', 0), coords.get('y', 0), coords.get('z', 0))
                        nearest_sector = find_nearest_sector(system_coords, sector_centers)
                        
                        if nearest_sector != "Unknown":
                            system_line = json.dumps(system, separators=(',', ':')) + '\n'
                            assignment_batches[nearest_sector].append(system_line)
                            assigned += 1
                            
                            # Flush batch if it gets large
                            if len(assignment_batches[nearest_sector]) >= 1000:
                                _flush_sector_batch_worker(output_dir, nearest_sector, assignment_batches[nearest_sector])
                                assignment_batches[nearest_sector].clear()
                        else:
                            skipped += 1
                    else:
                        skipped += 1
                        
                except json.JSONDecodeError:
                    skipped += 1
                    continue
        
        # Final flush
        for sector_name, lines in assignment_batches.items():
            if lines:
                _flush_sector_batch_worker(output_dir, sector_name, lines)
        
        return {'assigned': assigned, 'skipped': skipped}
    
    
    def _calculate_sector_ranges(self) -> List[Tuple[int, int, int, int, int, int]]:
        """Calculate sector ranges for parallel processing.
        
        Galaxy spans roughly -65,000 to +65,000 LY in each dimension.
        Divide into sectors of 2000 LY (2 sectors per 1000 LY unit) for parallelization.
        
        Returns:
            List of (min_x, max_x, min_y, max_y, min_z, max_z) tuples
        """
        ranges = []
        
        # Divide galaxy into manageable chunks
        # Each chunk covers 10,000 LY (10 sector units) in each dimension
        chunk_size = 10
        
        for x in range(-65, 65, chunk_size):
            for y in range(-65, 65, chunk_size):
                for z in range(-65, 65, chunk_size):
                    ranges.append((x, x + chunk_size - 1, 
                                 y, y + chunk_size - 1,
                                 z, z + chunk_size - 1))
                    
                    # Don't create too many ranges
                    if len(ranges) >= self.max_workers * 4:
                        return ranges
        
        return ranges
    
    def _write_build_metadata(self, stats: BuildStats, dataset_type: str, 
                            source_file: Path) -> None:
        """Write metadata about the build process."""
        metadata = {
            'build_time': time.time(),
            'build_duration_seconds': stats.build_time_seconds,
            'dataset_type': dataset_type,
            'source_file': str(source_file),
            'source_size_mb': stats.original_size_mb,
            'compressed_size_mb': stats.compressed_size_mb,
            'compression_ratio_percent': stats.compression_ratio,
            'total_systems': stats.total_systems,
            'total_stations': stats.total_stations,
            'sectors_created': stats.sectors_created,
            'max_workers': self.max_workers,
            'batch_size': self.batch_size
        }
        
        metadata_file = self.output_base_dir / "build_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        logger.info(f"Build metadata written to: {metadata_file}")
    
    def verify_database(self) -> Dict[str, Any]:
        """Verify database integrity and provide statistics.
        
        Returns:
            Verification results
        """
        logger.info("Verifying database integrity...")
        
        results = {
            'valid': True,
            'errors': [],
            'statistics': {}
        }
        
        # Check sector files
        sector_files = list(self.galaxy_sectors_dir.glob("*.jsonl.gz"))
        
        if not sector_files:
            results['valid'] = False
            results['errors'].append("No sector files found")
            return results
            
        logger.info(f"Found {len(sector_files)} sector files")
        
        # Sample verification of files
        total_systems = 0
        total_stations = 0
        corrupt_files = 0
        
        sample_size = min(10, len(sector_files))
        for sector_file in sector_files[:sample_size]:
            try:
                with gzip.open(sector_file, 'rt') as f:
                    for line in f:
                        if line.strip():
                            system = json.loads(line)
                            total_systems += 1
                            total_stations += len(system.get('stations', []))
                            
            except Exception as e:
                corrupt_files += 1
                results['errors'].append(f"Corrupt file {sector_file}: {e}")
                
        if corrupt_files > 0:
            results['valid'] = False
            
        results['statistics'] = {
            'total_sector_files': len(sector_files),
            'sampled_files': sample_size,
            'corrupt_files': corrupt_files,
            'sample_systems': total_systems,
            'sample_stations': total_stations
        }
        
        logger.info(f"Database verification complete: "
                   f"{'VALID' if results['valid'] else 'INVALID'}")
        
        return results