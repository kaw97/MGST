#!/usr/bin/env python3
"""
Convert codex.json array format to JSONL format for efficient processing.
"""

import json
import sys
from pathlib import Path

def convert_codex_to_jsonl(input_file: Path, output_file: Path, chunk_size: int = 10000):
    """Convert large JSON array to JSONL format."""

    print(f"Converting {input_file} to JSONL format...")
    print(f"Output: {output_file}")

    # Read and parse the JSON array incrementally
    entries_written = 0

    with open(input_file, 'r', encoding='utf-8') as infile:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            # Skip the opening bracket
            char = infile.read(1)
            if char != '[':
                raise ValueError("Expected JSON array to start with '['")

            entry_buffer = ""
            bracket_count = 0
            in_string = False
            escape_next = False

            while True:
                char = infile.read(1)
                if not char:  # EOF
                    break

                # Handle string escaping
                if escape_next:
                    entry_buffer += char
                    escape_next = False
                    continue

                if char == '\\':
                    entry_buffer += char
                    escape_next = True
                    continue

                if char == '"' and not escape_next:
                    in_string = not in_string

                if not in_string:
                    if char == '{':
                        bracket_count += 1
                    elif char == '}':
                        bracket_count -= 1

                entry_buffer += char

                # End of entry
                if not in_string and bracket_count == 0 and char == '}':
                    # We have a complete entry
                    try:
                        # Parse to validate JSON
                        entry = json.loads(entry_buffer.strip())
                        # Write as JSONL
                        json.dump(entry, outfile, ensure_ascii=False)
                        outfile.write('\n')
                        entries_written += 1

                        if entries_written % chunk_size == 0:
                            print(f"Converted {entries_written:,} entries...", file=sys.stderr)

                    except json.JSONDecodeError as e:
                        print(f"Skipping invalid JSON entry at position {entries_written}: {e}", file=sys.stderr)

                    # Reset for next entry
                    entry_buffer = ""

                    # Skip comma and whitespace
                    while True:
                        next_char = infile.read(1)
                        if not next_char:
                            break
                        if next_char in ',\n\r\t ':
                            continue
                        elif next_char == ']':
                            # End of array
                            break
                        else:
                            # Start of next entry
                            entry_buffer = next_char
                            break

    print(f"Conversion complete! Wrote {entries_written:,} entries to {output_file}")
    return entries_written

def main():
    input_file = Path('Databases/codex.json/codex.json')
    output_file = Path('Databases/codex.json/codex.jsonl')

    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    # Check if output already exists
    if output_file.exists():
        print(f"Output file already exists: {output_file}")
        response = input("Overwrite? (y/N): ").strip().lower()
        if response != 'y':
            print("Aborted.")
            return

    try:
        convert_codex_to_jsonl(input_file, output_file)
    except Exception as e:
        print(f"Error during conversion: {e}")
        if output_file.exists():
            output_file.unlink()  # Clean up partial file

if __name__ == "__main__":
    main()