#!/usr/bin/env python3
"""
Compress the entire sector database using gzip.
Creates a new compressed database while keeping the original for testing.
"""

import gzip
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import shutil

def compress_single_file(args):
    """
    Compress a single sector file.
    
    Args:
        args: Tuple of (input_file, output_file, worker_id)
        
    Returns:
        Tuple of (success, input_size, output_size, processing_time, filename)
    """
    input_file, output_file, worker_id = args
    
    start_time = time.time()
    
    try:
        input_size = input_file.stat().st_size
        
        # Compress with good compression level (6 is default balance of speed/size)
        with open(input_file, 'rb') as f_in:
            with gzip.open(output_file, 'wb', compresslevel=6) as f_out:
                # Use larger buffer for better performance
                buffer_size = 64 * 1024 * 1024  # 64MB buffer
                while True:
                    chunk = f_in.read(buffer_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
        
        output_size = output_file.stat().st_size
        processing_time = time.time() - start_time
        
        return True, input_size, output_size, processing_time, input_file.name
        
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"❌ Worker {worker_id} failed to compress {input_file.name}: {e}")
        return False, 0, 0, processing_time, input_file.name


def main():
    """Compress the entire sector database."""
    source_dir = Path("Databases/galaxy_sectors")
    target_dir = Path("Databases/galaxy_sectors_compressed")
    
    print("🗜️  HITEC Galaxy Sector Database Compression")
    print("=" * 60)
    print()
    
    # Validate source directory
    if not source_dir.exists():
        print(f"❌ Source directory not found: {source_dir}")
        return 1
    
    # Check if sector index exists
    sector_index = source_dir / "sector_index.json"
    if not sector_index.exists():
        print(f"❌ Sector index not found: {sector_index}")
        print("   Make sure the sector database is complete")
        return 1
    
    # Create target directory
    target_dir.mkdir(exist_ok=True)
    
    # Get list of sector files to compress
    sector_files = list(source_dir.glob("*.jsonl"))
    
    if not sector_files:
        print(f"❌ No sector files found in {source_dir}")
        return 1
    
    print(f"📁 Source: {source_dir}")
    print(f"📁 Target: {target_dir}")
    print(f"📊 Files to compress: {len(sector_files):,}")
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in sector_files)
    total_size_gb = total_size / (1024**3)
    print(f"📦 Total size: {total_size_gb:.1f} GB")
    print()
    
    # Prepare compression tasks
    worker_args = []
    for i, input_file in enumerate(sector_files):
        output_file = target_dir / (input_file.name + '.gz')
        worker_args.append((input_file, output_file, f"W{i%12:02d}"))
    
    # Use 12 workers for parallel compression
    max_workers = 12
    print(f"🚀 Starting compression with {max_workers} workers...")
    print("   Note: This will take significant time and CPU resources")
    print()
    
    start_time = time.time()
    completed_files = 0
    total_input_size = 0
    total_output_size = 0
    failed_files = []
    
    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all compression tasks
            future_to_file = {
                executor.submit(compress_single_file, args): args[0]
                for args in worker_args
            }
            
            # Process results as they complete
            for future in as_completed(future_to_file):
                input_file = future_to_file[future]
                
                try:
                    success, input_size, output_size, proc_time, filename = future.result()
                    
                    if success:
                        completed_files += 1
                        total_input_size += input_size
                        total_output_size += output_size
                        
                        # Calculate compression stats
                        compression_ratio = output_size / input_size if input_size > 0 else 0
                        space_saved = (1 - compression_ratio) * 100
                        
                        # Progress update every 100 files
                        if completed_files % 100 == 0:
                            elapsed = time.time() - start_time
                            rate = completed_files / elapsed if elapsed > 0 else 0
                            overall_ratio = total_output_size / total_input_size if total_input_size > 0 else 0
                            overall_saved = (1 - overall_ratio) * 100
                            
                            print(f"📈 Progress: {completed_files:,}/{len(sector_files):,} "
                                  f"({completed_files/len(sector_files)*100:.1f}%) "
                                  f"at {rate:.1f} files/sec")
                            print(f"   Overall compression: {overall_saved:.1f}% space saved")
                            print()
                    else:
                        failed_files.append(filename)
                        
                except Exception as e:
                    failed_files.append(input_file.name)
                    print(f"❌ Error processing {input_file.name}: {e}")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Compression interrupted by user")
        print(f"   Completed: {completed_files}/{len(sector_files)} files")
        return 1
    
    total_time = time.time() - start_time
    
    # Copy sector index to compressed directory
    try:
        shutil.copy2(sector_index, target_dir / "sector_index.json")
        print(f"✅ Copied sector index to compressed database")
    except Exception as e:
        print(f"⚠️  Warning: Could not copy sector index: {e}")
    
    # Final statistics
    print(f"🎉 Compression Complete!")
    print("=" * 60)
    print(f"⏱️  Total time: {total_time/3600:.1f} hours ({total_time:.0f} seconds)")
    print(f"📊 Files processed: {completed_files:,}/{len(sector_files):,}")
    print(f"❌ Failed files: {len(failed_files)}")
    
    if total_input_size > 0:
        input_gb = total_input_size / (1024**3)
        output_gb = total_output_size / (1024**3)
        overall_ratio = total_output_size / total_input_size
        space_saved_gb = input_gb - output_gb
        space_saved_percent = (1 - overall_ratio) * 100
        
        print()
        print(f"📦 Size Reduction:")
        print(f"   Original: {input_gb:.1f} GB")
        print(f"   Compressed: {output_gb:.1f} GB")
        print(f"   Space saved: {space_saved_gb:.1f} GB ({space_saved_percent:.1f}%)")
        print(f"   Compression ratio: {overall_ratio:.3f}")
        
        # Performance stats
        if total_time > 0:
            throughput_gb = input_gb / (total_time / 3600)  # GB per hour
            print(f"   Throughput: {throughput_gb:.1f} GB/hour")
    
    if failed_files:
        print(f"\n⚠️  Failed files:")
        for filename in failed_files[:10]:  # Show first 10 failures
            print(f"   - {filename}")
        if len(failed_files) > 10:
            print(f"   ... and {len(failed_files) - 10} more")
    
    print(f"\n📁 Compressed database location: {target_dir}")
    print(f"📁 Original database preserved: {source_dir}")
    print()
    print("💡 To use the compressed database:")
    print(f"   hitec-galaxy filter --input-dir {target_dir} ...")
    
    return 0 if len(failed_files) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())