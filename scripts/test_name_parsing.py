#!/usr/bin/env python3
"""
Test the system name parsing logic on a small sample.
"""

import json
import re
from pathlib import Path

def parse_system_name(name):
    """Parse a system name to extract sector and mass code."""
    # Mass code pattern: 2 letters, dash, letter (e.g., SG-E, AB-C, XY-Z)
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    
    match = re.search(mass_code_pattern, name)
    if match:
        mass_code = match.group(1)
        mass_code_start = match.start()
        
        # Sector is everything before the mass code (trimmed)
        sector = name[:mass_code_start].strip()
        
        # Rest is everything after the mass code
        rest = name[match.end():].strip()
        
        return sector, mass_code, rest
    else:
        # Doesn't follow standard pattern
        return None, None, name

def test_parsing():
    """Test parsing on a small sample from the first file."""
    database_dir = Path("Databases/galaxy_chunks_annotated")
    first_file = list(database_dir.glob("*.jsonl"))[0]
    
    print(f"Testing parsing with {first_file.name}")
    print("=" * 80)
    
    standard_count = 0
    non_standard_count = 0
    examples = {'standard': [], 'non_standard': []}
    
    with open(first_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 1000:  # Test first 1000 systems
                break
                
            line = line.strip()
            if not line:
                continue
            
            try:
                system_data = json.loads(line)
                system_name = system_data.get('name', '')
                
                if not system_name:
                    continue
                
                sector, mass_code, rest = parse_system_name(system_name)
                
                if sector is not None and mass_code is not None:
                    standard_count += 1
                    if len(examples['standard']) < 10:
                        examples['standard'].append({
                            'name': system_name,
                            'sector': sector,
                            'mass_code': mass_code,
                            'rest': rest
                        })
                else:
                    non_standard_count += 1
                    if len(examples['non_standard']) < 10:
                        examples['non_standard'].append(system_name)
            
            except json.JSONDecodeError:
                continue
    
    print(f"Results from first {standard_count + non_standard_count} systems:")
    print(f"Standard naming: {standard_count} ({standard_count/(standard_count+non_standard_count)*100:.1f}%)")
    print(f"Non-standard: {non_standard_count} ({non_standard_count/(standard_count+non_standard_count)*100:.1f}%)")
    
    print(f"\nStandard naming examples:")
    for example in examples['standard']:
        print(f"  '{example['name']}' -> Sector: '{example['sector']}', Mass: '{example['mass_code']}', Rest: '{example['rest']}'")
    
    print(f"\nNon-standard naming examples:")
    for example in examples['non_standard']:
        print(f"  '{example}'")

if __name__ == "__main__":
    test_parsing()