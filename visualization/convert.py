#!/usr/bin/env python3
"""
Convert CSV bus data to GeoJSON format for Mapbox visualization
"""

import csv
import json
import sys
import os

def csv_to_geojson(csv_file, geojson_file, skip_duplicates=True):
    """
    Convert CSV file with longitude, latitude, speed to GeoJSON
    
    Args:
        csv_file (str): Path to input CSV file
        geojson_file (str): Path to output GeoJSON file
        skip_duplicates (bool): Whether to skip duplicate coordinates
    """
    
    features = []
    seen_coordinates = set()  # Track duplicates
    skipped_count = 0
    processed_count = 0
    
    print(f"Converting {csv_file} to {geojson_file}...")
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            # Auto-detect delimiter (comma or tab)
            sample = file.read(1024)
            file.seek(0)
            
            if '\t' in sample:
                delimiter = '\t'
                print("Detected tab-separated file")
            else:
                delimiter = ','
                print("Detected comma-separated file")
            
            reader = csv.DictReader(file, delimiter=delimiter)
            
            # Print headers found
            print(f"Headers found: {reader.fieldnames}")
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Clean and extract data
                    longitude = float(row['longitude'].strip('"'))
                    latitude = float(row['latitude'].strip('"'))
                    speed = float(row['speed'].strip('"')) if row['speed'].strip('"') else 0
                    
                    # Skip invalid coordinates
                    if longitude == 0 and latitude == 0:
                        continue
                    
                    # Skip duplicates if requested
                    coord_key = (longitude, latitude)
                    if skip_duplicates and coord_key in seen_coordinates:
                        skipped_count += 1
                        continue
                    
                    seen_coordinates.add(coord_key)
                    
                    # Create GeoJSON feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [longitude, latitude]
                        },
                        "properties": {
                            "speed": speed
                        }
                    }
                    
                    features.append(feature)
                    processed_count += 1
                    
                    # Progress indicator
                    if processed_count % 1000 == 0:
                        print(f"Processed {processed_count} records...")
                
                except (ValueError, KeyError) as e:
                    print(f"Warning: Skipping row {row_num} due to error: {e}")
                    continue
    
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found!")
        return False
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return False
    
    # Create GeoJSON structure
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    # Write to output file
    try:
        with open(geojson_file, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, separators=(',', ':'))  # Compact format
        
        print(f"\n✅ Conversion successful!")
        print(f"📊 Statistics:")
        print(f"   - Total features: {len(features)}")
        print(f"   - Records processed: {processed_count}")
        if skip_duplicates:
            print(f"   - Duplicates skipped: {skipped_count}")
        print(f"   - Output file: {geojson_file}")
        print(f"   - File size: {os.path.getsize(geojson_file):,} bytes")
        
        return True
        
    except Exception as e:
        print(f"Error writing GeoJSON file: {e}")
        return False

def main():
    """Main function with command line interface"""
    
    # Default file names
    default_csv = "bus_data.csv"
    default_geojson = "bus_data.geojson"
    
    # Check command line arguments
    if len(sys.argv) >= 2:
        csv_file = sys.argv[1]
    else:
        csv_file = default_csv
    
    if len(sys.argv) >= 3:
        geojson_file = sys.argv[2]
    else:
        # Generate output filename based on input
        base_name = os.path.splitext(csv_file)[0]
        geojson_file = f"{base_name}.geojson"
    
    # Check if input file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: Input file '{csv_file}' not found!")
        print(f"\nUsage: python3 {sys.argv[0]} [input.csv] [output.geojson]")
        print(f"Example: python3 {sys.argv[0]} last_5_days.csv bus_map.geojson")
        return
    
    # Convert the file
    success = csv_to_geojson(csv_file, geojson_file, skip_duplicates=True)
    
    if success:
        print(f"\n🗺️  Ready for Mapbox! Use '{geojson_file}' in your HTML file.")
    else:
        print(f"\n❌ Conversion failed!")

if __name__ == "__main__":
    main()