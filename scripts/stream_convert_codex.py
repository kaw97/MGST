#!/usr/bin/env python3
"""
Stream convert codex.json array to JSONL format with progress monitoring.
"""

import json
import ijson
import sys
from pathlib import Path
from datetime import datetime

def stream_convert_codex(input_file: Path, output_file: Path):
    """Stream convert JSON array to JSONL with progress monitoring."""

    print(f"Starting streaming conversion: {input_file} -> {output_file}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    entries_written = 0

    try:
        with open(input_file, 'rb') as infile:
            with open(output_file, 'w', encoding='utf-8') as outfile:
                # Stream parse the JSON array
                parser = ijson.items(infile, 'item')

                for entry in parser:
                    # Write each entry as a JSON line
                    json.dump(entry, outfile, ensure_ascii=False, separators=(',', ':'))
                    outfile.write('\n')
                    entries_written += 1

                    # Progress reporting
                    if entries_written % 50000 == 0:
                        current_time = datetime.now().strftime('%H:%M:%S')
                        print(f"[{current_time}] Converted {entries_written:,} entries...")
                        sys.stdout.flush()

    except Exception as e:
        print(f"Error during conversion: {e}")
        return False

    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Conversion complete!")
    print(f"End time: {end_time}")
    print(f"Total entries: {entries_written:,}")
    print(f"Output file: {output_file}")

    return True

def main():
    input_file = Path('Databases/codex.json/codex.json')
    output_file = Path('Databases/codex.json/codex.jsonl')

    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    # Create output directory if needed
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Check if output already exists
    if output_file.exists():
        print(f"Output file already exists: {output_file}")
        print(f"Size: {output_file.stat().st_size / (1024*1024*1024):.2f} GB")
        return

    success = stream_convert_codex(input_file, output_file)

    if success:
        # Show final file info
        size_gb = output_file.stat().st_size / (1024*1024*1024)
        print(f"Final file size: {size_gb:.2f} GB")
    else:
        # Clean up on failure
        if output_file.exists():
            output_file.unlink()
            print("Cleaned up partial file due to error")

if __name__ == "__main__":
    main()