"""Data format conversion utilities."""

import json
import pandas as pd
from decimal import Decimal
from pathlib import Path
from typing import Optional, Union, Any

try:
    import ijson
    HAS_IJSON = True
except ImportError:
    HAS_IJSON = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def decimal_default(obj: Any) -> float:
    """JSON serializer for Decimal objects.
    
    Args:
        obj: Object to serialize
        
    Returns:
        Float representation of Decimal objects
        
    Raises:
        TypeError: If object is not JSON serializable
    """
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def convert_json_to_jsonl(input_file: Path, output_file: Optional[Path] = None) -> Path:
    """Convert JSON array file to JSONL format.
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSONL file (default: input with .jsonl extension)
        
    Returns:
        Path to output file
        
    Raises:
        ImportError: If ijson is not available for large file processing
        FileNotFoundError: If input file doesn't exist
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if output_file is None:
        output_file = input_file.with_suffix('.jsonl')
    
    # Use ijson for memory-efficient processing of large JSON arrays
    if HAS_IJSON:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            # ijson.items parses each item in the top-level array incrementally
            objects = ijson.items(infile, 'item')
            
            # Progress bar if available
            if HAS_TQDM:
                objects = tqdm(objects, desc="Converting to JSONL")
            
            for obj in objects:
                json.dump(obj, outfile, default=decimal_default, separators=(',', ':'))
                outfile.write('\\n')
    else:
        # Fallback to standard json module (loads entire file into memory)
        print("Warning: ijson not available, loading entire file into memory")
        with open(input_file, 'r', encoding='utf-8') as infile:
            data = json.load(infile)
        
        if not isinstance(data, list):
            raise ValueError("Input JSON must be an array for JSONL conversion")
        
        with open(output_file, 'w', encoding='utf-8') as outfile:
            items = tqdm(data, desc="Converting to JSONL") if HAS_TQDM else data
            for obj in items:
                json.dump(obj, outfile, default=decimal_default, separators=(',', ':'))
                outfile.write('\\n')
    
    print(f"Converted {input_file} -> {output_file}")
    return output_file


def convert_jsonl_to_json(input_file: Path, output_file: Optional[Path] = None) -> Path:
    """Convert JSONL format file to JSON array.
    
    Args:
        input_file: Path to input JSONL file
        output_file: Path to output JSON file (default: input with .json extension)
        
    Returns:
        Path to output file
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if output_file is None:
        output_file = input_file.with_suffix('.json')
    
    objects = []
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        lines = infile.readlines()
        if HAS_TQDM:
            lines = tqdm(lines, desc="Converting to JSON")
        
        for line in lines:
            line = line.strip()
            if line:
                try:
                    obj = json.loads(line)
                    objects.append(obj)
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON line: {e}")
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(objects, outfile, default=decimal_default, indent=2)
    
    print(f"Converted {input_file} -> {output_file}")
    return output_file


def fix_excel_numbers(input_file: Path, output_file: Optional[Path] = None) -> Path:
    """Fix numerical formatting in TSV files to prevent Excel import issues.
    
    Args:
        input_file: Path to input TSV file
        output_file: Path to output TSV file (default: input with _fixed suffix)
        
    Returns:
        Path to output file
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if output_file is None:
        output_file = input_file.parent / f"{input_file.stem}_fixed{input_file.suffix}"
    
    # Read the TSV file
    df = pd.read_csv(input_file, sep='\\t')
    
    # Round numerical columns to reasonable precision
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            if col in ['total_distance', 'avg_distance_per_jump']:
                # Round distances to 1 decimal place
                df[col] = df[col].round(1)
            elif col in ['center_x', 'center_y', 'center_z', 'distance_to_origin', 
                         'coords_x', 'coords_y', 'coords_z']:
                # Coordinate columns - round to 1 decimal place
                df[col] = df[col].round(1)
            elif col in ['cluster_id', 'system_count', 'qualifying_bodies', 'total_genera']:
                # Integer columns - ensure they stay as integers
                df[col] = df[col].astype('Int64')  # Nullable integer type
            elif col in ['total_value', 'body_1_value', 'body_2_value', 'body_3_value']:
                # Large values - round to nearest integer
                df[col] = df[col].round(0).astype('Int64')
            elif col in ['body_1_pressure', 'body_2_pressure', 'body_3_pressure']:
                # Pressure values - round to 3 decimal places
                df[col] = df[col].round(3)
    
    # Write back to TSV
    df.to_csv(output_file, sep='\\t', index=False)
    print(f"Fixed numerical formatting: {input_file} -> {output_file}")
    
    return output_file


def convert_tsv_to_excel(input_file: Path, output_file: Optional[Path] = None) -> Path:
    """Convert TSV file to Excel format with proper formatting.
    
    Args:
        input_file: Path to input TSV file
        output_file: Path to output Excel file (default: input with .xlsx extension)
        
    Returns:
        Path to output file
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if output_file is None:
        output_file = input_file.with_suffix('.xlsx')
    
    # Read TSV file
    df = pd.read_csv(input_file, sep='\\t')
    
    # Write to Excel with formatting
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Systems')
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['Systems']
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    print(f"Converted {input_file} -> {output_file}")
    return output_file


def auto_detect_and_convert(
    input_file: Path, 
    target_format: str, 
    output_file: Optional[Path] = None
) -> Path:
    """Auto-detect input format and convert to target format.
    
    Args:
        input_file: Path to input file
        target_format: Target format ('json', 'jsonl', 'tsv', 'csv', 'xlsx')
        output_file: Path to output file (auto-generated if None)
        
    Returns:
        Path to output file
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    input_format = input_file.suffix.lower().lstrip('.')
    
    if output_file is None:
        output_file = input_file.with_suffix(f'.{target_format}')
    
    print(f"Converting {input_format.upper()} -> {target_format.upper()}")
    
    # Direct conversions
    if input_format == 'json' and target_format == 'jsonl':
        return convert_json_to_jsonl(input_file, output_file)
    elif input_format == 'jsonl' and target_format == 'json':
        return convert_jsonl_to_json(input_file, output_file)
    elif input_format == 'tsv' and target_format == 'xlsx':
        return convert_tsv_to_excel(input_file, output_file)
    
    # Multi-step conversions using pandas
    if input_format in ['tsv', 'csv']:
        # Read with pandas
        sep = '\\t' if input_format == 'tsv' else ','
        df = pd.read_csv(input_file, sep=sep)
        
        if target_format == 'json':
            df.to_json(output_file, orient='records', indent=2)
        elif target_format == 'jsonl':
            df.to_json(output_file, orient='records', lines=True)
        elif target_format == 'csv':
            df.to_csv(output_file, index=False)
        elif target_format == 'tsv':
            df.to_csv(output_file, sep='\\t', index=False)
        elif target_format == 'xlsx':
            df.to_excel(output_file, index=False)
        else:
            raise ValueError(f"Unsupported target format: {target_format}")
    
    elif input_format in ['json', 'jsonl']:
        # Read JSON data
        if input_format == 'json':
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = [data]  # Wrap single object in list
        else:  # jsonl
            data = []
            with open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line.strip()))
        
        df = pd.DataFrame(data)
        
        if target_format == 'csv':
            df.to_csv(output_file, index=False)
        elif target_format == 'tsv':
            df.to_csv(output_file, sep='\\t', index=False)
        elif target_format == 'xlsx':
            df.to_excel(output_file, index=False)
        else:
            raise ValueError(f"Unsupported conversion: {input_format} -> {target_format}")
    
    else:
        raise ValueError(f"Unsupported input format: {input_format}")
    
    print(f"Converted {input_file} -> {output_file}")
    return output_file