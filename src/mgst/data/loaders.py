"""Data loading utilities for various formats."""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator, Union

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def load_systems_from_tsv(
    file_path: Path, 
    required_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Load system data from TSV file.
    
    Args:
        file_path: Path to TSV file
        required_columns: List of required columns to validate
        
    Returns:
        DataFrame with system data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If required columns are missing
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    df = pd.read_csv(file_path, sep='\\t')
    
    if required_columns:
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
    
    return df


def load_systems_from_jsonl(file_path: Path) -> Iterator[Dict[str, Any]]:
    """Load system data from JSONL file as an iterator.
    
    Args:
        file_path: Path to JSONL file
        
    Yields:
        Dictionary for each system
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
                continue


def load_systems_from_jsonl_batch(
    file_path: Path, 
    batch_size: int = 1000
) -> Iterator[List[Dict[str, Any]]]:
    """Load system data from JSONL file in batches.
    
    Args:
        file_path: Path to JSONL file
        batch_size: Number of systems per batch
        
    Yields:
        List of system dictionaries for each batch
    """
    batch = []
    
    for system in load_systems_from_jsonl(file_path):
        batch.append(system)
        
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Yield remaining items
    if batch:
        yield batch


def load_systems_from_json(file_path: Path) -> List[Dict[str, Any]]:
    """Load system data from JSON array file.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        List of system dictionaries
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If JSON is not an array
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of systems")
    
    return data


def load_systems_from_directory(
    directory: Path,
    file_pattern: str = "*.jsonl",
    progress: bool = True
) -> Iterator[Dict[str, Any]]:
    """Load system data from all files matching pattern in directory.
    
    Args:
        directory: Directory containing data files
        file_pattern: Glob pattern for files to load
        progress: Whether to show progress bar
        
    Yields:
        Dictionary for each system across all files
    """
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    files = list(directory.glob(file_pattern))
    if not files:
        print(f"Warning: No files found matching {file_pattern} in {directory}")
        return
    
    if progress and HAS_TQDM:
        files = tqdm(files, desc="Loading files")
    
    for file_path in files:
        if file_path.suffix.lower() == '.jsonl':
            yield from load_systems_from_jsonl(file_path)
        elif file_path.suffix.lower() == '.json':
            systems = load_systems_from_json(file_path)
            yield from systems
        else:
            print(f"Warning: Unsupported file format: {file_path}")


def count_systems_in_file(file_path: Path) -> int:
    """Count the number of systems in a data file.
    
    Args:
        file_path: Path to data file
        
    Returns:
        Number of systems in the file
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if file_path.suffix.lower() == '.jsonl':
        count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    count += 1
        return count
    
    elif file_path.suffix.lower() == '.json':
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return len(data) if isinstance(data, list) else 1
    
    elif file_path.suffix.lower() in ['.tsv', '.csv']:
        sep = '\\t' if file_path.suffix.lower() == '.tsv' else ','
        # Use pandas to count rows efficiently
        df = pd.read_csv(file_path, sep=sep, usecols=[0])  # Read only first column
        return len(df)
    
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


def count_systems_in_directory(
    directory: Path,
    file_pattern: str = "*.jsonl"
) -> int:
    """Count total systems across all files in directory.
    
    Args:
        directory: Directory containing data files
        file_pattern: Glob pattern for files to count
        
    Returns:
        Total number of systems across all files
    """
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    
    files = list(directory.glob(file_pattern))
    total_count = 0
    
    for file_path in files:
        try:
            count = count_systems_in_file(file_path)
            total_count += count
            print(f"{file_path.name}: {count:,} systems")
        except Exception as e:
            print(f"Warning: Error counting {file_path}: {e}")
    
    return total_count


def validate_system_data(system: Dict[str, Any]) -> bool:
    """Validate that a system dictionary has required structure.
    
    Args:
        system: System data dictionary
        
    Returns:
        True if system data is valid, False otherwise
    """
    # Check for required fields
    if 'name' not in system:
        return False
    
    # Check coordinates structure
    if 'coords' not in system:
        return False
    
    coords = system['coords']
    if not isinstance(coords, dict):
        return False
    
    required_coord_fields = ['x', 'y', 'z']
    if not all(field in coords for field in required_coord_fields):
        return False
    
    # Check that coordinates are numeric
    try:
        float(coords['x'])
        float(coords['y']) 
        float(coords['z'])
    except (ValueError, TypeError):
        return False
    
    # Bodies should be a list if present
    if 'bodies' in system and not isinstance(system['bodies'], list):
        return False
    
    return True


def load_proximity_systems(file_path: Path) -> List[str]:
    """Load a list of system names from a text file.
    
    Args:
        file_path: Path to text file with one system name per line
        
    Returns:
        List of system names
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    systems = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                systems.append(line)
    
    return systems