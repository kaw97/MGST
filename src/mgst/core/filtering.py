"""Galaxy system filtering with configuration support."""

import json
import logging
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Callable, Optional
import gc
from dataclasses import dataclass

from ..configs.base import BaseConfig
from .spatial import SpatialRange, SectorIndex, SpatialPrefilter
from ..data.compressed_reader import CompressedFileReader

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

@dataclass
class FilteringResult:
    """Results from galaxy filtering operation."""
    files_processed: int
    systems_processed: int
    matches_found: int
    processing_time: float
    errors: List[str]
    
    @property
    def match_rate(self) -> float:
        """Calculate match rate percentage."""
        return (self.matches_found / self.systems_processed * 100) if self.systems_processed > 0 else 0.0
    
    @property
    def processing_speed(self) -> float:
        """Calculate processing speed in systems per second."""
        return self.systems_processed / self.processing_time if self.processing_time > 0 else 0.0


class ProgressHandler:
    """Progress bar handler that works with or without tqdm."""
    
    def __init__(self, total: int, desc: str = "Processing"):
        self.total = total
        self.desc = desc
        if HAS_TQDM:
            self.pbar = tqdm(total=total, desc=desc, unit="files")
        else:
            self.pbar = None
            self.current = 0
            self._last_update = 0
            
    def update(self, n: int = 1):
        """Update progress by n units."""
        if HAS_TQDM and self.pbar:
            self.pbar.update(n)
        else:
            self.current += n
            now = time.time()
            if now - self._last_update >= 1.0:  # Update every second
                percent = (self.current / self.total * 100) if self.total > 0 else 0
                print(f"\r{self.desc}: {self.current}/{self.total} ({percent:.1f}%)", end='', flush=True)
                self._last_update = now
                
    def close(self):
        """Close the progress handler."""
        if HAS_TQDM and self.pbar:
            self.pbar.close()
        else:
            print()  # New line after progress



class StreamingTSVWriter:
    """Memory-efficient TSV writer that handles streaming output."""
    
    def __init__(self, output_path: Path, config: BaseConfig):
        self.output_path = output_path
        self.config = config
        self.file_handle = None
        self.header_written = False
        
    def __enter__(self):
        self.file_handle = open(self.output_path, 'w', encoding='utf-8')
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file_handle:
            self.file_handle.close()
    
    def write_header(self):
        """Write TSV header."""
        if not self.header_written:
            columns = self.config.get_output_columns()
            header = '\t'.join(columns)
            self.file_handle.write(header + '\n')
            self.file_handle.flush()
            self.header_written = True
    
    def write_result(self, result: Dict[str, Any]):
        """Write a single result to the TSV file."""
        if not self.header_written:
            self.write_header()
            
        columns = self.config.get_output_columns()
        row = []
        for col_name in columns:
            value = result.get(col_name, '')
            # Convert to string and handle tabs/newlines
            str_value = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            row.append(str_value)
        self.file_handle.write('\t'.join(row) + '\n')
        self.file_handle.flush()


def write_result_to_file(result: Dict[str, Any], system_data: Dict[str, Any], 
                        output_path: str, output_format: str, config: BaseConfig):
    """Write a single result to the output file with file locking."""
    import fcntl
    
    try:
        if output_format.lower() == 'tsv':
            # Write TSV format
            with open(output_path, 'a', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                
                # Check if file is empty (need header)
                import os
                if os.path.getsize(output_path) == 0:
                    columns = config.get_output_columns()
                    header = '\t'.join(columns)
                    f.write(header + '\n')
                
                # Write data row
                columns = config.get_output_columns()
                row = []
                for col_name in columns:
                    value = result.get(col_name, '')
                    # Convert to string and handle tabs/newlines
                    str_value = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                    row.append(str_value)
                f.write('\t'.join(row) + '\n')
                f.flush()
                
        elif output_format.lower() == 'jsonl':
            # Write JSONL format
            with open(output_path, 'a', encoding='utf-8') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                # Write the complete system record
                f.write(json.dumps(system_data) + '\n')
                f.flush()
                
    except Exception as e:
        # Silently handle write errors to avoid breaking workers
        pass


def process_jsonl_file(args: Tuple) -> Dict[str, Any]:
    """Process a single JSONL file with filtering.
    
    Args:
        args: Tuple containing processing parameters
        
    Returns:
        Dictionary with processing results
    """
    (input_file, config, chunk_size, test_mode, max_test_systems, 
     output_path, output_format, write_directly, spatial_prefilter) = args
    
    try:
        total_processed = 0
        matches_found = 0
        errors = []
        matched_systems = []
        
        # Use compressed file reader for transparent gzip support
        with CompressedFileReader(input_file, encoding='utf-8') as f:
            buffer = ""
            systems_processed_this_file = 0
            
            # Get compression info for statistics
            compression_info = f.get_compression_info()
            if compression_info['is_compressed']:
                compressed_size_mb = compression_info['compressed_size'] / (1024 * 1024)
                if compression_info['original_size']:
                    original_size_mb = compression_info['original_size'] / (1024 * 1024)
                    ratio = compression_info['compression_ratio']
                    print(f"  📦 Compressed file: {compressed_size_mb:.1f}MB → {original_size_mb:.1f}MB (ratio: {ratio:.2f})")
            
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                    
                buffer += chunk
                lines = buffer.split('\n')
                buffer = lines[-1]  # Keep incomplete line
                
                for line in lines[:-1]:
                    line = line.strip()
                    if not line:
                        continue
                        
                    try:
                        system_data = json.loads(line)
                        total_processed += 1
                        systems_processed_this_file += 1
                        
                        # Apply spatial pre-filtering if enabled
                        if spatial_prefilter and not spatial_prefilter.should_process_system(system_data):
                            continue
                        
                        # Apply filter
                        filtered_result = config.filter_system(system_data)
                        if filtered_result:
                            matches_found += 1
                            if write_directly and output_path:
                                # Write directly to file with locking
                                write_result_to_file(filtered_result, system_data, output_path, output_format, config)
                            else:
                                # Store result for batch writing
                                filtered_result['_complete_system_record'] = system_data
                                matched_systems.append(filtered_result)
                        
                        # Test mode limit
                        if test_mode and systems_processed_this_file >= max_test_systems:
                            break
                            
                    except json.JSONDecodeError as e:
                        errors.append(f"JSON decode error in {input_file}: {e}")
                        continue
                    except Exception as e:
                        errors.append(f"Filter error in {input_file} for system {system_data.get('name', 'Unknown')}: {e}")
                        continue
                
                if test_mode and systems_processed_this_file >= max_test_systems:
                    break
            
            # Process remaining buffer
            if buffer.strip() and not (test_mode and systems_processed_this_file >= max_test_systems):
                try:
                    system_data = json.loads(buffer.strip())
                    total_processed += 1
                    
                    # Apply spatial pre-filtering if enabled
                    if spatial_prefilter and not spatial_prefilter.should_process_system(system_data):
                        pass  # Skip this system
                    else:
                        filtered_result = config.filter_system(system_data)
                        if filtered_result:
                            matches_found += 1
                            if write_directly and output_path:
                                # Write directly to file with locking
                                write_result_to_file(filtered_result, system_data, output_path, output_format, config)
                            else:
                                filtered_result['_complete_system_record'] = system_data
                                matched_systems.append(filtered_result)
                        
                except json.JSONDecodeError as e:
                    errors.append(f"JSON decode error in {input_file} (final): {e}")
                except Exception as e:
                    errors.append(f"Filter error in {input_file} (final): {e}")
        
        # Force garbage collection
        gc.collect()
        
        return {
            'file': input_file.name,
            'matched_systems': matched_systems,
            'total_processed': total_processed,
            'matches_found': matches_found,
            'errors': errors
        }
        
    except Exception as e:
        return {
            'file': input_file.name,
            'matched_systems': [],
            'total_processed': 0,
            'matches_found': 0,
            'errors': [f"Worker process error: {e}"]
        }


def write_results(results: List[Dict], config: BaseConfig, output_path: Path, output_format: str):
    """Write filtering results to output file.
    
    Args:
        results: List of matched systems
        config: Configuration object with output columns
        output_path: Path to output file
        output_format: Output format ('tsv' or 'jsonl')
    """
    if not results:
        print("No matching systems found.")
        return
        
    if output_format.lower() == 'tsv':
        with StreamingTSVWriter(output_path, config) as writer:
            for result in results:
                writer.write_result(result)
                
    elif output_format.lower() == 'jsonl':
        with open(output_path, 'w', encoding='utf-8') as f:
            for system in results:
                # Write the complete system record if available, otherwise just the summary
                complete_record = system.get('_complete_system_record')
                if complete_record:
                    f.write(json.dumps(complete_record) + '\n')
                else:
                    f.write(json.dumps(system) + '\n')
                f.flush()
                
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def filter_galaxy_data(
    input_dir: Path,
    config: BaseConfig,
    output_path: Path,
    output_format: str = 'tsv',
    workers: int = 8,
    chunk_size: int = 10 * 1024 * 1024,
    test_mode: bool = False,
    max_test_systems: int = 1000,
    verbose: bool = False,
    spatial_prefilter: Optional[SpatialPrefilter] = None
) -> FilteringResult:
    """Filter galaxy data using a configuration.
    
    Args:
        input_dir: Directory containing JSONL files (or sector database if using spatial prefilter)
        config: Configuration object with filtering logic
        output_path: Output file path
        output_format: Output format ('tsv' or 'jsonl')
        workers: Number of worker processes
        chunk_size: Chunk size for reading files
        test_mode: Whether to run in test mode
        max_test_systems: Maximum systems to process in test mode
        verbose: Enable verbose logging
        spatial_prefilter: Optional spatial prefilter for sector-based filtering
        
    Returns:
        FilteringResult with processing statistics
    """
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Validate inputs
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    
    # Find input files - use spatial prefilter if provided
    if spatial_prefilter:
        input_files = spatial_prefilter.get_input_files()
        spatial_stats = spatial_prefilter.get_stats()
        print(f"Spatial prefiltering enabled:")
        print(f"  Target systems: {spatial_stats['target_systems_count']:,}")
        print(f"  Range: {spatial_stats['range_ly']:,.0f} ly")
        print(f"  Sectors: {spatial_stats['filtered_sectors']:,}/{spatial_stats['total_sectors']:,} "
              f"({spatial_stats['sector_reduction']:,.1f}% reduction)")
        print(f"  Systems: {spatial_stats['filtered_systems']:,}/{spatial_stats['total_systems']:,} "
              f"({spatial_stats['system_reduction']:,.1f}% reduction)")
    else:
        # Find both compressed and uncompressed JSONL files
        input_files = []
        input_files.extend(input_dir.glob("*.jsonl"))       # Uncompressed
        input_files.extend(input_dir.glob("*.jsonl.gz"))    # Gzip compressed
    
    if not input_files:
        raise FileNotFoundError(f"No JSONL files (compressed or uncompressed) found in {input_dir}")
    
    # Show compression statistics
    from ..data.compressed_reader import detect_compressed_files
    try:
        compression_stats = detect_compressed_files(input_dir, "*.jsonl*")
        if compression_stats['compressed_count'] > 0:
            total_gb = compression_stats['total_size'] / (1024**3)
            compressed_gb = compression_stats['total_compressed_size'] / (1024**3)
            uncompressed_gb = compression_stats['total_uncompressed_size'] / (1024**3)
            print(f"📦 Compression statistics:")
            print(f"  Files: {compression_stats['compressed_count']} compressed, {compression_stats['uncompressed_count']} uncompressed")
            print(f"  Sizes: {compressed_gb:.1f}GB compressed, {uncompressed_gb:.1f}GB uncompressed (total: {total_gb:.1f}GB)")
    except Exception:
        pass  # Don't fail if compression stats fail
    
    print(f"Found {len(input_files)} JSONL files to process")
    print(f"Configuration: {config.get_description()}")
    print(f"Output columns: {config.get_output_columns()}")
    
    if test_mode:
        print(f"TEST MODE: Processing first {max_test_systems} systems per file")
    
    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Process files in parallel
    start_time = time.time()
    total_processed = 0
    total_matches = 0
    all_errors = []
    all_matched_systems = []
    
    # Create empty output file if not in test mode
    if not test_mode:
        # Initialize output file (workers will append to it)
        if output_format.lower() == 'tsv':
            # Create empty file - first worker will write header
            with open(output_path, 'w', encoding='utf-8') as f:
                pass
        elif output_format.lower() == 'jsonl':
            # Create empty JSONL file
            with open(output_path, 'w', encoding='utf-8') as f:
                pass

    # Prepare worker arguments - enable streaming for non-test mode
    write_directly = not test_mode
    worker_args = [
        (input_file, config, chunk_size, test_mode, max_test_systems, 
         str(output_path) if not test_mode else "", output_format, write_directly, spatial_prefilter)
        for input_file in input_files
    ]
    
    print(f"Processing with {workers} workers...")
    
    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(process_jsonl_file, arg): arg[0]
                for arg in worker_args
            }
            
            # Process results as they complete
            progress = ProgressHandler(len(input_files), "Processing files")
            
            try:
                for future in as_completed(future_to_file):
                    progress.update(1)
                    
                    try:
                        result = future.result()
                        
                        total_processed += result['total_processed']
                        total_matches += result['matches_found']
                        all_errors.extend(result['errors'])
                        all_matched_systems.extend(result['matched_systems'])
                        
                        if verbose:
                            logger.info(f"File {result['file']}: {result['matches_found']} matches from {result['total_processed']} systems")
                            
                    except Exception as e:
                        file_path = future_to_file[future]
                        logger.error(f"Error processing {file_path}: {e}")
                        all_errors.append(f"Processing error for {file_path}: {e}")
            
            finally:
                progress.close()
    
    except Exception as e:
        all_errors.append(f"Processing pool error: {e}")
    
    processing_time = time.time() - start_time
    
    # Write results to output file (only needed for batch mode)
    if all_matched_systems and not test_mode and not write_directly:
        # Batch mode - write collected results
        write_results(all_matched_systems, config, output_path, output_format)
        print(f"Results written to: {output_path}")
    elif not test_mode and write_directly:
        # Streaming mode - results already written by workers
        if total_matches > 0:
            print(f"Results streamed to: {output_path}")
        else:
            print(f"No matches found - empty file: {output_path}")
    elif test_mode:
        print(f"TEST MODE: Found {total_matches} matches (results not saved)")
    
    # Create and return results
    result = FilteringResult(
        files_processed=len(input_files),
        systems_processed=total_processed,
        matches_found=total_matches,
        processing_time=processing_time,
        errors=all_errors
    )
    
    # Print statistics
    print(f"\n=== Processing Complete ===")
    print(f"Files processed: {result.files_processed}")
    print(f"Systems processed: {result.systems_processed:,}")
    print(f"Matches found: {result.matches_found:,}")
    print(f"Match rate: {result.match_rate:.3f}%")
    print(f"Processing time: {result.processing_time:.1f}s")
    print(f"Processing speed: {result.processing_speed:.0f} systems/sec")
    
    if result.errors:
        print(f"\nWarnings/Errors: {len(result.errors)}")
        if verbose:
            for error in result.errors[:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(result.errors) > 10:
                print(f"  ... and {len(result.errors) - 10} more errors")
    
    return result


def validate_config(config: BaseConfig, sample_system: Optional[Dict] = None) -> bool:
    """Validate a configuration by testing it.
    
    Args:
        config: Configuration to validate
        sample_system: Optional sample system data for testing
        
    Returns:
        True if config is valid, False otherwise
    """
    try:
        print(f"✓ Configuration loaded: {config.name}")
        print(f"✓ Description: {config.get_description()}")
        print(f"✓ Output columns ({len(config.get_output_columns())}): {config.get_output_columns()}")
        
        # Test with sample system if provided
        if sample_system:
            try:
                result = config.filter_system(sample_system)
                print(f"✓ Filter function test: returns {type(result).__name__}")
                
                if result:
                    print("✓ Sample system passes filter")
                    for col_name in config.get_output_columns()[:5]:  # Show first 5 columns
                        value = result.get(col_name, 'N/A')
                        print(f"  - {col_name}: {value}")
                else:
                    print("✓ Sample system filtered out")
                        
            except Exception as e:
                print(f"✗ Filter function test failed: {e}")
                return False
                
        return True
        
    except Exception as e:
        print(f"✗ Configuration validation failed: {e}")
        return False