"""High-performance downloader for Spansh galaxy data."""

import gzip
import json
import shutil
import subprocess
import requests
from pathlib import Path
from typing import Optional, Iterator, Dict, Any
import logging
from concurrent.futures import ThreadPoolExecutor
import hashlib

logger = logging.getLogger(__name__)


class SpanshDownloader:
    """Downloads and validates Spansh galaxy data with streaming support."""
    
    SPANSH_URLS = {
        'full': 'https://downloads.spansh.co.uk/galaxy.json.gz',
        '1month': 'https://downloads.spansh.co.uk/galaxy_1month.json.gz',
        '7days': 'https://downloads.spansh.co.uk/galaxy_7days.json.gz',
        '1day': 'https://downloads.spansh.co.uk/galaxy_1day.json.gz'
    }
    
    def __init__(self, download_dir: Path, chunk_size: int = 64 * 1024 * 1024):
        """Initialize downloader.
        
        Args:
            download_dir: Directory to store downloaded files
            chunk_size: Download chunk size in bytes (default 64MB)
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        
    def download_dataset(self, dataset_type: str = 'full', 
                        force_redownload: bool = False) -> Path:
        """Download Spansh dataset.
        
        Args:
            dataset_type: Type of dataset ('full', '1month', '7days', '1day')
            force_redownload: Force redownload even if file exists
            
        Returns:
            Path to downloaded file
        """
        if dataset_type not in self.SPANSH_URLS:
            raise ValueError(f"Unknown dataset type: {dataset_type}")
            
        url = self.SPANSH_URLS[dataset_type]
        filename = f"galaxy_{dataset_type}.json.gz"
        file_path = self.download_dir / filename
        
        # Check if file already exists and is valid
        if file_path.exists() and not force_redownload:
            if self._validate_gzip_file(file_path):
                logger.info(f"Using existing file: {file_path}")
                return file_path
            else:
                logger.warning(f"Existing file corrupt, redownloading: {file_path}")
                
        logger.info(f"Downloading {dataset_type} dataset from {url}")
        
        # Download with streaming
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        if total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            logger.info(f"Download progress: {progress:.1f}% "
                                      f"({downloaded_size / (1024**3):.1f}GB)")
        
        # Validate downloaded file
        if not self._validate_gzip_file(file_path):
            raise ValueError(f"Downloaded file is corrupt: {file_path}")
            
        logger.info(f"Successfully downloaded: {file_path}")
        return file_path
    
    @staticmethod
    def _has_pigz() -> bool:
        """Check if pigz is available on the system."""
        return shutil.which('pigz') is not None

    def stream_systems(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """Stream systems from compressed JSON file.

        Uses pigz for parallel decompression when available, falling back
        to Python's gzip module otherwise.

        Args:
            file_path: Path to compressed JSON file

        Yields:
            System dictionaries
        """
        try:
            import ijson
        except ImportError:
            raise ImportError("ijson is required for streaming JSON parsing")

        logger.info(f"Streaming systems from: {file_path}")

        use_pigz = self._has_pigz()
        proc = None
        stream = None

        try:
            if use_pigz:
                logger.info("Using pigz for parallel decompression")
                proc = subprocess.Popen(
                    ['pigz', '-dc', str(file_path)],
                    stdout=subprocess.PIPE,
                    bufsize=64 * 1024 * 1024,
                )
                stream = proc.stdout
            else:
                logger.info("Using Python gzip for decompression (install pigz for faster parallel decompression)")
                stream = gzip.open(file_path, 'rb')

            # Use yajl2_c backend if available for better performance
            try:
                parser = ijson.items(stream, 'item', use_float=True)
            except Exception:
                # Fallback to default backend
                parser = ijson.items(stream, 'item')

            count = 0
            for system in parser:
                # Convert Decimal coordinates to floats for consistency
                if 'coords' in system and system['coords']:
                    coords = system['coords']
                    # Handle both float and Decimal types
                    system['coords'] = {
                        'x': float(coords['x']),
                        'y': float(coords['y']),
                        'z': float(coords['z'])
                    }

                yield system
                count += 1

                # Log less frequently for large files
                if count % 50000 == 0:
                    logger.info(f"Processed {count:,} systems")
        finally:
            if proc is not None:
                proc.stdout.close()
                proc.wait()
            elif stream is not None:
                stream.close()

        logger.info(f"Finished streaming {count:,} systems")

    def stream_raw_systems(self, file_path: Path,
                           chunk_size: int = 4 * 1024 * 1024) -> Iterator[bytes]:
        """Stream raw JSON byte strings for each system object.

        Reads decompressed bytes from pigz (or gzip fallback) and uses a
        byte-level state machine to find top-level ``{...}`` object
        boundaries without parsing JSON.  Each yielded value is a raw
        ``bytes`` object containing exactly one complete JSON system
        object, suitable for later parsing with ``orjson.loads()``.

        Args:
            file_path: Path to compressed JSON file (.json.gz).
            chunk_size: Read chunk size in bytes (default 4 MB).

        Yields:
            Raw JSON bytes for each system object.
        """
        logger.info(f"Streaming raw system objects from: {file_path}")

        use_pigz = self._has_pigz()
        proc = None
        stream = None

        try:
            if use_pigz:
                logger.info("Using pigz for parallel decompression (raw mode)")
                proc = subprocess.Popen(
                    ['pigz', '-dc', str(file_path)],
                    stdout=subprocess.PIPE,
                    bufsize=64 * 1024 * 1024,
                )
                stream = proc.stdout
            else:
                logger.info("Using Python gzip for decompression (raw mode)")
                stream = gzip.open(file_path, 'rb')

            # State machine
            brace_depth = 0
            in_string = False
            escape_next = False

            # Accumulator for objects spanning chunk boundaries
            accumulator = bytearray()
            obj_start = -1  # position within current chunk; -1 = not inside object

            count = 0

            while True:
                chunk = stream.read(chunk_size)
                if not chunk:
                    break

                i = 0
                chunk_len = len(chunk)

                while i < chunk_len:
                    # --- fast-path: skip to next '{' when outside objects ---
                    if brace_depth == 0 and not in_string:
                        next_brace = chunk.find(b'{', i)
                        if next_brace == -1:
                            break  # no object starts in remainder of chunk
                        i = next_brace
                        # fall through to handle the '{'

                    b = chunk[i]

                    if escape_next:
                        escape_next = False
                        i += 1
                        continue

                    if b == 0x5C and in_string:  # backslash inside string
                        escape_next = True
                        i += 1
                        continue

                    if b == 0x22:  # double quote
                        in_string = not in_string
                        i += 1
                        continue

                    if in_string:
                        i += 1
                        continue

                    # Outside a string
                    if b == 0x7B:  # '{'
                        if brace_depth == 0:
                            obj_start = i
                        brace_depth += 1
                    elif b == 0x7D:  # '}'
                        brace_depth -= 1
                        if brace_depth == 0:
                            # Complete object found
                            if accumulator:
                                accumulator.extend(
                                    chunk[obj_start if obj_start >= 0 else 0 : i + 1]
                                )
                                yield bytes(accumulator)
                                accumulator.clear()
                            else:
                                yield chunk[obj_start : i + 1]

                            obj_start = -1
                            count += 1

                            if count % 50000 == 0:
                                logger.info(
                                    f"Extracted {count:,} raw system objects"
                                )

                    i += 1

                # End of chunk: if inside an object, carry over to next chunk
                if brace_depth > 0:
                    if obj_start >= 0:
                        accumulator.extend(chunk[obj_start:])
                        obj_start = 0
                    else:
                        # Continuation from previous chunk, whole chunk is part
                        # of the current object
                        accumulator.extend(chunk)
                else:
                    obj_start = -1

        finally:
            if proc is not None:
                proc.stdout.close()
                proc.wait()
            elif stream is not None:
                stream.close()

        logger.info(f"Finished extracting {count:,} raw system objects")

    def get_file_info(self, file_path: Path) -> Dict[str, Any]:
        """Get information about a downloaded file.
        
        Args:
            file_path: Path to file
            
        Returns:
            File information dictionary
        """
        if not file_path.exists():
            return {'exists': False}
            
        stat = file_path.stat()
        
        info = {
            'exists': True,
            'size_bytes': stat.st_size,
            'size_gb': stat.st_size / (1024**3),
            'modified_time': stat.st_mtime,
            'is_valid': self._validate_gzip_file(file_path)
        }
        
        # Try to get system count by sampling
        if info['is_valid']:
            try:
                sample_count = 0
                for system in self.stream_systems(file_path):
                    sample_count += 1
                    if sample_count >= 1000:  # Sample first 1000
                        break
                        
                # Estimate total count based on file size and sample
                if sample_count > 0:
                    avg_system_size = stat.st_size / 10  # Rough estimate
                    info['estimated_system_count'] = int(stat.st_size / avg_system_size)
                    
            except Exception as e:
                logger.warning(f"Could not estimate system count: {e}")
                
        return info
    
    def _validate_gzip_file(self, file_path: Path) -> bool:
        """Validate that gzip file can be opened and read.
        
        Args:
            file_path: Path to gzip file
            
        Returns:
            True if file is valid
        """
        try:
            with gzip.open(file_path, 'rb') as f:
                # Try to read first few bytes
                f.read(1024)
            return True
        except Exception as e:
            logger.error(f"File validation failed for {file_path}: {e}")
            return False
    
    def download_all_datasets(self, max_workers: int = 2) -> Dict[str, Path]:
        """Download all available datasets in parallel.
        
        Args:
            max_workers: Maximum number of download workers
            
        Returns:
            Dictionary mapping dataset types to file paths
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit download jobs
            futures = {}
            for dataset_type in self.SPANSH_URLS.keys():
                future = executor.submit(self.download_dataset, dataset_type)
                futures[future] = dataset_type
                
            # Collect results
            for future in futures:
                dataset_type = futures[future]
                try:
                    file_path = future.result()
                    results[dataset_type] = file_path
                    logger.info(f"Successfully downloaded {dataset_type}")
                except Exception as e:
                    logger.error(f"Failed to download {dataset_type}: {e}")
                    
        return results