#!/usr/bin/env python3
"""
Extract trip_id and route_number pairs from TriMet stop events HTML data.
"""

import re
import sys

def extract_trip_route_mapping(html_content):
    """
    Extract [trip_id, route_id] pairs from HTML content.
    
    Args:
        html_content: HTML string containing stop events data
        
    Returns:
        List of [trip_id, route_number] pairs
    """
    trip_route_pairs = []
    
    # Split content by trip sections
    # Look for headers like "Stop events for PDX_TRIP 245529922"
    trip_sections = re.split(r'<h2>Stop events for PDX_TRIP\s+(\d+)</h2>', html_content)
    
    # Process each trip section
    for i in range(1, len(trip_sections), 2):  # Skip first empty section, then take every other
        trip_id = trip_sections[i]  # The captured trip ID
        table_content = trip_sections[i + 1] if i + 1 < len(trip_sections) else ""
        
        # Find the first data row to get route_number
        # Look for the first <tr><td> after the header row
        data_row_match = re.search(r'<tr><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td>', table_content)
        
        if data_row_match:
            # The route_number is typically the 4th column (index 3) based on your data structure
            # Columns: vehicle_number, leave_time, train, route_number
            route_number = data_row_match.group(4)
            trip_route_pairs.append([int(trip_id), int(route_number)])
        else:
            print(f"Warning: Could not find route number for trip {trip_id}")
    
    return trip_route_pairs

def extract_trip_route_mapping_simple(html_content):
    """
    Simpler approach: find trip headers and look for route numbers in the following table.
    """
    trip_route_pairs = []
    
    # Find all trip headers with their trip IDs
    trip_matches = re.finditer(r'<h2>Stop events for PDX_TRIP\s+(\d+)</h2>', html_content)
    
    for match in trip_matches:
        trip_id = int(match.group(1))
        start_pos = match.end()
        
        # Find the next table after this header
        table_match = re.search(r'<table>.*?<tr><td.*?</td><td.*?</td><td.*?</td><td.*?(\d+)</td>', 
                               html_content[start_pos:], re.DOTALL)
        
        if table_match:
            route_number = int(table_match.group(1))
            trip_route_pairs.append([trip_id, route_number])
        else:
            print(f"Warning: Could not find route number for trip {trip_id}")
    
    return trip_route_pairs

def main():
    """Main function to process HTML file or content."""
    if len(sys.argv) != 2:
        print("Usage: python extract_trip_route.py <html_file>")
        print("   or: python extract_trip_route.py '<html_content>'")
        sys.exit(1)
    
    input_arg = sys.argv[1]
    
    # Check if it's a file path or direct HTML content
    try:
        # Try to read as file first
        with open(input_arg, 'r') as f:
            html_content = f.read()
        print(f"Reading from file: {input_arg}")
    except FileNotFoundError:
        # Treat as direct HTML content
        html_content = input_arg
        print("Processing HTML content directly")
    
    # Extract trip-route pairs
    trip_route_pairs = extract_trip_route_mapping_simple(html_content)
    
    # Print results
    print("\nTrip ID -> Route Number mappings:")
    print("=" * 40)
    for trip_id, route_number in trip_route_pairs:
        print(f"Trip {trip_id} -> Route {route_number}")
    
    # Also print as Python list format
    print(f"\nAs Python list:")
    print("trip_route_mappings = [")
    for trip_id, route_number in trip_route_pairs:
        print(f"    [{trip_id}, {route_number}],")
    print("]")
    
    return trip_route_pairs

if __name__ == "__main__":
    main()