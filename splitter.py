#!/usr/bin/env python3
"""
Simple tool to split a JSONL file into two equal parts.
Usage: python split_jsonl.py input_file.jsonl
"""

import sys
import os

def split_jsonl_in_half(input_file):
    """Split a JSONL file into two equal parts."""
    
    # Check if file exists
    if not os.path.exists(input_file):
        print(f"Error: File '{input_file}' not found.")
        return
    
    # Get base filename without extension
    base_name = os.path.splitext(input_file)[0]
    
    print(f"Splitting {input_file}...")
    
    # Count total lines first
    print("Counting lines...")
    with open(input_file, 'r') as f:
        total_lines = sum(1 for _ in f)
    
    print(f"Total lines: {total_lines}")
    
    # Calculate split point
    split_point = total_lines // 2
    
    # Create output filenames
    file1 = f"{base_name}_part1.jsonl"
    file2 = f"{base_name}_part2.jsonl"
    
    print(f"Part 1: {file1} (lines 1-{split_point})")
    print(f"Part 2: {file2} (lines {split_point + 1}-{total_lines})")
    
    # Split the file
    with open(input_file, 'r') as input_f:
        with open(file1, 'w') as out1, open(file2, 'w') as out2:
            for line_num, line in enumerate(input_f, 1):
                if line_num <= split_point:
                    out1.write(line)
                else:
                    out2.write(line)
    
    # Show results
    size1 = os.path.getsize(file1) / (1024 * 1024)
    size2 = os.path.getsize(file2) / (1024 * 1024)
    
    print(f"\nSplit complete!")
    print(f"{file1}: {split_point} lines, {size1:.1f}MB")
    print(f"{file2}: {total_lines - split_point} lines, {size2:.1f}MB")

def main():
    if len(sys.argv) != 2:
        print("Usage: python split_jsonl.py <input_file.jsonl>")
        print("\nExample: python split_jsonl.py breadcrumbs.jsonl")
        print("Creates: breadcrumbs_part1.jsonl and breadcrumbs_part2.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    split_jsonl_in_half(input_file)

if __name__ == "__main__":
    main()