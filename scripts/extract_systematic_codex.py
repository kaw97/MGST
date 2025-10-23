#!/usr/bin/env python3
"""
Extract and organize systematic codex entries by sector.
Creates sector-specific JSONL files containing only systematic entries.
This pre-filtering dramatically reduces the enrichment workload.
"""

import json
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import argparse

def parse_system_name(system_name: str) -> str:
    """Extract sector name from systematic system naming."""
    mass_code_pattern = r'\b([A-Z]{2}-[A-Z])\b'
    match = re.search(mass_code_pattern, system_name)

    if match:
        mass_code_start = match.start()
        sector_name = system_name[:mass_code_start].strip()
        return sector_name
    return None

def extract_systematic_entries(codex_file: Path, output_dir: Path, max_entries: int = None):
    """Extract systematic entries and organize by sector."""

    print("🔍 Extracting systematic codex entries...")
    print(f"📂 Input: {codex_file}")
    print(f"📁 Output directory: {output_dir}")
    if max_entries:
        print(f"🔢 Max entries: {max_entries:,}")
    print("=" * 60)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Statistics
    total_entries = 0
    systematic_entries = 0
    sector_counts = defaultdict(int)
    sector_files = {}

    # Open sector files as needed
    def get_sector_file(sector_name):
        if sector_name not in sector_files:
            sanitized_name = sector_name.replace(' ', '_').replace('/', '_')
            sector_file_path = output_dir / f"{sanitized_name}.jsonl"
            sector_files[sector_name] = open(sector_file_path, 'w', encoding='utf-8')
            print(f"📝 Created sector file: {sanitized_name}.jsonl")
        return sector_files[sector_name]

    print("🚀 Processing codex entries...")

    try:
        with open(codex_file, 'r', encoding='utf-8') as f:
            for line in f:
                if max_entries and total_entries >= max_entries:
                    break

                total_entries += 1

                # Progress indicator
                if total_entries % 100000 == 0:
                    print(f"📊 Processed {total_entries:,} entries, found {systematic_entries:,} systematic ({systematic_entries/total_entries*100:.1f}%)")

                try:
                    entry = json.loads(line.strip())
                    system_name = entry.get('system', '')
                    sector_name = parse_system_name(system_name)

                    if sector_name:
                        # Write to sector-specific file
                        sector_file = get_sector_file(sector_name)
                        sector_file.write(json.dumps(entry, ensure_ascii=False) + '\n')

                        sector_counts[sector_name] += 1
                        systematic_entries += 1

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"❌ Error processing entry: {e}")
                    continue

    finally:
        # Close all sector files
        for sector_file in sector_files.values():
            sector_file.close()

    # Create summary
    summary = {
        "extraction_timestamp": datetime.now().isoformat(),
        "source_file": str(codex_file),
        "total_entries_processed": total_entries,
        "systematic_entries_found": systematic_entries,
        "systematic_percentage": systematic_entries / total_entries * 100 if total_entries > 0 else 0,
        "unique_sectors": len(sector_counts),
        "sector_distribution": dict(sector_counts)
    }

    summary_file = output_dir / "extraction_summary.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Final statistics
    print(f"\n✅ Systematic entry extraction complete!")
    print(f"📊 Summary:")
    print(f"   • Total entries processed: {total_entries:,}")
    print(f"   • Systematic entries found: {systematic_entries:,}")
    print(f"   • Systematic percentage: {systematic_entries/total_entries*100:.1f}%")
    print(f"   • Unique sectors: {len(sector_counts):,}")
    print(f"   • Average entries per sector: {systematic_entries/len(sector_counts):.1f}")

    # Top sectors by entry count
    top_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    print(f"\n🔝 Top 10 sectors by entry count:")
    for sector, count in top_sectors:
        print(f"   • {sector}: {count:,} entries")

    print(f"\n📁 Sector files saved to: {output_dir}")
    print(f"📄 Summary saved to: {summary_file}")

    return len(sector_counts), systematic_entries

def main():
    parser = argparse.ArgumentParser(description='Extract systematic codex entries by sector')
    parser.add_argument('--max-entries', type=int, help='Limit number of codex entries to process')
    args = parser.parse_args()

    codex_file = Path('Databases/codex.json/codex.jsonl')
    output_dir = Path(f'output/systematic_codex_{datetime.now().strftime("%Y%m%d_%H%M%S")}')

    if not codex_file.exists():
        print(f"❌ Codex file not found: {codex_file}")
        return

    extract_systematic_entries(codex_file, output_dir, args.max_entries)

if __name__ == "__main__":
    main()