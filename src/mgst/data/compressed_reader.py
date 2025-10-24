"""
Compressed file reader for HITEC Galaxy.
Provides transparent gzip decompression while maintaining streaming capabilities.
"""

import gzip
import os
from pathlib import Path
from typing import TextIO, Union, Optional
import io


class CompressedFileReader:
    """
    A file reader that transparently handles gzip-compressed files.
    Maintains streaming capabilities and is compatible with existing chunk-based processing.
    """
    
    def __init__(self, file_path: Union[str, Path], encoding: str = 'utf-8', 
                 buffer_size: int = 64 * 1024 * 1024):  # 64MB buffer for better performance
        """
        Initialize compressed file reader.
        
        Args:
            file_path: Path to file (compressed or uncompressed)
            encoding: Text encoding (default: utf-8)
            buffer_size: Internal buffer size for decompression (default: 64MB)
        """
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.buffer_size = buffer_size
        self.file_handle: Optional[TextIO] = None
        self.is_compressed = False
        self.original_size: Optional[int] = None
        self.compressed_size: Optional[int] = None
        
    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def _detect_compression(self) -> bool:
        """
        Detect if file is gzip compressed.
        
        Returns:
            True if file is gzip compressed, False otherwise
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        # Check file extension first
        if self.file_path.suffix.lower() == '.gz':
            return True
        
        # Check for multiple extensions like .jsonl.gz
        if str(self.file_path).lower().endswith('.jsonl.gz'):
            return True
        
        # Check magic bytes (more reliable than extension)
        try:
            with open(self.file_path, 'rb') as f:
                magic = f.read(2)
                return magic == b'\x1f\x8b'  # gzip magic bytes
        except IOError:
            return False
    
    def open(self):
        """Open the file with appropriate decompression."""
        if self.file_handle:
            return  # Already open
            
        self.is_compressed = self._detect_compression()
        self.compressed_size = self.file_path.stat().st_size
        
        if self.is_compressed:
            # Open gzip file with larger buffer for better performance
            self.file_handle = gzip.open(
                self.file_path, 
                'rt', 
                encoding=self.encoding,
                compresslevel=6,  # Default compression level
                newline=None      # Handle different line endings
            )
            
            # Try to get original size from gzip header (last 4 bytes)
            try:
                with open(self.file_path, 'rb') as f:
                    f.seek(-4, 2)  # Go to last 4 bytes
                    self.original_size = int.from_bytes(f.read(4), byteorder='little')
            except (OSError, ValueError):
                self.original_size = None
        else:
            # Open regular file
            self.file_handle = open(self.file_path, 'r', encoding=self.encoding)
            self.original_size = self.compressed_size
    
    def close(self):
        """Close the file handle."""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
    
    def read(self, size: int = -1) -> str:
        """
        Read data from file with transparent decompression.
        
        Args:
            size: Number of characters to read (-1 for all)
            
        Returns:
            Decompressed text data
        """
        if not self.file_handle:
            raise ValueError("File not open. Use 'with' statement or call open() first.")
        
        return self.file_handle.read(size)
    
    def readline(self) -> str:
        """Read a single line from the file."""
        if not self.file_handle:
            raise ValueError("File not open. Use 'with' statement or call open() first.")
        
        return self.file_handle.readline()
    
    def readlines(self):
        """Read all lines from the file."""
        if not self.file_handle:
            raise ValueError("File not open. Use 'with' statement or call open() first.")
        
        return self.file_handle.readlines()
    
    def __iter__(self):
        """Iterate over lines in the file."""
        if not self.file_handle:
            raise ValueError("File not open. Use 'with' statement or call open() first.")
        
        return iter(self.file_handle)
    
    def get_compression_info(self) -> dict:
        """
        Get information about file compression.
        
        Returns:
            Dictionary with compression statistics
        """
        info = {
            'is_compressed': self.is_compressed,
            'compressed_size': self.compressed_size,
            'original_size': self.original_size,
            'compression_ratio': None,
            'space_saved': None
        }
        
        if self.is_compressed and self.original_size and self.compressed_size:
            info['compression_ratio'] = self.compressed_size / self.original_size
            info['space_saved'] = 1 - info['compression_ratio']
        
        return info


def open_file_compressed(file_path: Union[str, Path], encoding: str = 'utf-8', 
                        buffer_size: int = 64 * 1024 * 1024) -> CompressedFileReader:
    """
    Convenience function to open a file with transparent gzip support.
    
    Args:
        file_path: Path to file (compressed or uncompressed)
        encoding: Text encoding (default: utf-8)
        buffer_size: Buffer size for decompression (default: 64MB)
        
    Returns:
        CompressedFileReader instance
    """
    return CompressedFileReader(file_path, encoding, buffer_size)


def detect_compressed_files(directory: Union[str, Path], pattern: str = "*.jsonl*") -> dict:
    """
    Detect compressed and uncompressed files in a directory.
    
    Args:
        directory: Directory to scan
        pattern: File pattern to match (default: "*.jsonl*")
        
    Returns:
        Dictionary with file lists and statistics
    """
    directory = Path(directory)
    
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"Directory not found or not a directory: {directory}")
    
    all_files = list(directory.glob(pattern))
    compressed_files = []
    uncompressed_files = []
    total_compressed_size = 0
    total_uncompressed_size = 0
    
    for file_path in all_files:
        reader = CompressedFileReader(file_path)
        is_compressed = reader._detect_compression()
        file_size = file_path.stat().st_size
        
        if is_compressed:
            compressed_files.append(file_path)
            total_compressed_size += file_size
        else:
            uncompressed_files.append(file_path)
            total_uncompressed_size += file_size
    
    return {
        'all_files': all_files,
        'compressed_files': compressed_files,
        'uncompressed_files': uncompressed_files,
        'total_files': len(all_files),
        'compressed_count': len(compressed_files),
        'uncompressed_count': len(uncompressed_files),
        'total_compressed_size': total_compressed_size,
        'total_uncompressed_size': total_uncompressed_size,
        'total_size': total_compressed_size + total_uncompressed_size
    }