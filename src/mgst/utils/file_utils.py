"""File handling utilities."""

import re
from pathlib import Path
from typing import Optional

def sanitize_filename(name: str, max_length: int = 50) -> str:
    """Sanitize system name for use in filename.
    
    Args:
        name: Original filename/system name
        max_length: Maximum length for the sanitized name
        
    Returns:
        Sanitized filename safe for filesystem use
    """
    # Remove or replace unsafe characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    
    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
    
    return sanitized

def ensure_output_dir(output_path: Path) -> Path:
    """Ensure output directory exists.
    
    Args:
        output_path: Path to output directory
        
    Returns:
        Path object for the created directory
    """
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path

def get_file_stem_with_prefix(filepath: Path, prefix: str) -> str:
    """Get filename stem with added prefix.
    
    Args:
        filepath: Original file path
        prefix: Prefix to add
        
    Returns:
        New filename with prefix
    """
    return f"{prefix}_{filepath.stem}"