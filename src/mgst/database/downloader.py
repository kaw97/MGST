"""High-performance downloader for Spansh galaxy data."""

import gzip
import json
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
    
    def stream_systems(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """Stream systems from compressed JSON file.
        
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
        
        # Use larger buffer for better performance with big compressed files
        with gzip.open(file_path, 'rb', compresslevel=1) as gz_file:
            # Use yajl2_c backend if available for better performance
            try:
                parser = ijson.items(gz_file, 'item', use_float=True)
            except Exception:
                # Fallback to default backend
                parser = ijson.items(gz_file, 'item')
            
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
                    
        logger.info(f"Finished streaming {count:,} systems")
    
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