"""Data validation utilities."""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import json


def validate_coordinates(coords: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate coordinate data structure and values.
    
    Args:
        coords: Coordinate dictionary with x, y, z fields
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    if not isinstance(coords, dict):
        errors.append("Coordinates must be a dictionary")
        return False, errors
    
    required_fields = ['x', 'y', 'z']
    for field in required_fields:
        if field not in coords:
            errors.append(f"Missing coordinate field: {field}")
        else:
            try:
                value = float(coords[field])
                # Elite Dangerous galaxy bounds check (roughly ±65,000 LY)
                if abs(value) > 100000:  # Allow some margin
                    errors.append(f"Coordinate {field}={value} seems outside galaxy bounds")
            except (ValueError, TypeError):
                errors.append(f"Coordinate {field} must be numeric, got: {coords[field]}")
    
    return len(errors) == 0, errors


def validate_system_data(system: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate complete system data structure.
    
    Args:
        system: System data dictionary
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Check required fields
    if 'name' not in system:
        errors.append("Missing system name")
    elif not isinstance(system['name'], str) or not system['name'].strip():
        errors.append("System name must be a non-empty string")
    
    # Validate coordinates
    if 'coords' not in system:
        errors.append("Missing coordinates")
    else:
        coord_valid, coord_errors = validate_coordinates(system['coords'])
        errors.extend(coord_errors)
    
    # Validate bodies if present
    if 'bodies' in system:
        if not isinstance(system['bodies'], list):
            errors.append("Bodies must be a list")
        else:
            for i, body in enumerate(system['bodies']):
                body_valid, body_errors = validate_body_data(body)
                if not body_valid:
                    errors.extend([f"Body {i}: {error}" for error in body_errors])
    
    return len(errors) == 0, errors


def validate_body_data(body: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate planetary body data structure.
    
    Args:
        body: Body data dictionary
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Check for body name
    if 'bodyName' not in body and 'name' not in body:
        errors.append("Missing body name")
    
    # Validate atmospheric data if present
    if 'atmosphereType' in body:
        if not isinstance(body['atmosphereType'], str):
            errors.append("Atmosphere type must be a string")
    
    if 'surfacePressure' in body:
        try:
            pressure = float(body['surfacePressure'])
            if pressure < 0:
                errors.append("Surface pressure cannot be negative")
        except (ValueError, TypeError):
            errors.append("Surface pressure must be numeric")
    
    # Validate temperature if present
    if 'surfaceTemperature' in body:
        try:
            temp = float(body['surfaceTemperature'])
            if temp < 0:  # Kelvin scale
                errors.append("Temperature cannot be negative (Kelvin scale)")
            elif temp > 10000:  # Sanity check
                errors.append("Temperature seems unreasonably high")
        except (ValueError, TypeError):
            errors.append("Surface temperature must be numeric")
    
    # Validate gravity if present
    if 'gravity' in body:
        try:
            gravity = float(body['gravity'])
            if gravity < 0:
                errors.append("Gravity cannot be negative")
            elif gravity > 50:  # Sanity check (Earth = 1.0)
                errors.append("Gravity seems unreasonably high")
        except (ValueError, TypeError):
            errors.append("Gravity must be numeric")
    
    # Validate bioscan predictions if present
    if 'bioscan_predictions' in body:
        bioscan_valid, bioscan_errors = validate_bioscan_data(body['bioscan_predictions'])
        errors.extend(bioscan_errors)
    
    return len(errors) == 0, errors


def validate_bioscan_data(bioscan: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate bioscan prediction data structure.
    
    Args:
        bioscan: Bioscan data dictionary
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    if not isinstance(bioscan, dict):
        errors.append("Bioscan predictions must be a dictionary")
        return False, errors
    
    if 'predicted_species' in bioscan:
        if not isinstance(bioscan['predicted_species'], list):
            errors.append("Predicted species must be a list")
        else:
            for i, species in enumerate(bioscan['predicted_species']):
                species_valid, species_errors = validate_species_data(species)
                if not species_valid:
                    errors.extend([f"Species {i}: {error}" for error in species_errors])
    
    return len(errors) == 0, errors


def validate_species_data(species: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate species prediction data structure.
    
    Args:
        species: Species data dictionary
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Check required fields
    required_fields = ['genus', 'species', 'value']
    for field in required_fields:
        if field not in species:
            errors.append(f"Missing species field: {field}")
    
    # Validate genus and species names
    if 'genus' in species and not isinstance(species['genus'], str):
        errors.append("Genus must be a string")
    
    if 'species' in species and not isinstance(species['species'], str):
        errors.append("Species must be a string")
    
    # Validate value
    if 'value' in species:
        try:
            value = float(species['value'])
            if value < 0:
                errors.append("Species value cannot be negative")
            elif value > 100000000:  # 100M credits seems like a reasonable upper limit
                errors.append("Species value seems unreasonably high")
        except (ValueError, TypeError):
            errors.append("Species value must be numeric")
    
    return len(errors) == 0, errors


def validate_tsv_file(file_path: Path, required_columns: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
    """Validate TSV file structure and content.
    
    Args:
        file_path: Path to TSV file
        required_columns: List of required column names
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    if not file_path.exists():
        errors.append(f"File not found: {file_path}")
        return False, errors
    
    try:
        df = pd.read_csv(file_path, sep='\\t')
    except Exception as e:
        errors.append(f"Error reading TSV file: {e}")
        return False, errors
    
    if len(df) == 0:
        errors.append("TSV file is empty")
    
    # Check required columns
    if required_columns:
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
    
    # Validate coordinate columns if present
    coord_cols = ['coords_x', 'coords_y', 'coords_z']
    present_coord_cols = [col for col in coord_cols if col in df.columns]
    
    if present_coord_cols:
        if len(present_coord_cols) != 3:
            errors.append("If coordinate columns are present, all three (x, y, z) must be included")
        else:
            for col in present_coord_cols:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    errors.append(f"Coordinate column {col} must be numeric")
    
    return len(errors) == 0, errors


def validate_jsonl_file(file_path: Path, max_lines_to_check: int = 100) -> Tuple[bool, List[str]]:
    """Validate JSONL file structure and content.
    
    Args:
        file_path: Path to JSONL file
        max_lines_to_check: Maximum number of lines to validate (for performance)
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    if not file_path.exists():
        errors.append(f"File not found: {file_path}")
        return False, errors
    
    lines_checked = 0
    valid_systems = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if lines_checked >= max_lines_to_check:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    system_data = json.loads(line)
                    system_valid, system_errors = validate_system_data(system_data)
                    if system_valid:
                        valid_systems += 1
                    else:
                        if len(errors) < 10:  # Limit error reporting
                            errors.append(f"Line {line_num}: {'; '.join(system_errors)}")
                
                except json.JSONDecodeError as e:
                    if len(errors) < 10:
                        errors.append(f"Line {line_num}: Invalid JSON - {e}")
                
                lines_checked += 1
    
    except Exception as e:
        errors.append(f"Error reading JSONL file: {e}")
        return False, errors
    
    if lines_checked == 0:
        errors.append("JSONL file appears to be empty")
    elif valid_systems == 0:
        errors.append("No valid system records found")
    
    # Add summary if we found some issues
    if errors and valid_systems > 0:
        errors.insert(0, f"Found {valid_systems} valid systems out of {lines_checked} checked")
    
    return len(errors) == 0, errors


def validate_clustering_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate DataFrame for clustering operations.
    
    Args:
        df: DataFrame with system data
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    if len(df) == 0:
        errors.append("DataFrame is empty")
        return False, errors
    
    # Check required columns for clustering
    required_cols = ['system_name', 'coords_x', 'coords_y', 'coords_z']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns for clustering: {missing_cols}")
    
    # Check for numeric coordinate columns
    coord_cols = ['coords_x', 'coords_y', 'coords_z']
    for col in coord_cols:
        if col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(f"Coordinate column {col} must be numeric")
            elif df[col].isna().any():
                errors.append(f"Coordinate column {col} contains missing values")
    
    # Check for duplicate system names
    if 'system_name' in df.columns:
        duplicates = df['system_name'].duplicated().sum()
        if duplicates > 0:
            errors.append(f"Found {duplicates} duplicate system names")
    
    return len(errors) == 0, errors