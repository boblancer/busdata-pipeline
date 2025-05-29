#!/usr/bin/env python3
"""
Simple HTML parser for TriMet stop events data.
"""

import re
import logging

logger = logging.getLogger('html_parser')

def parse_stop_events_html(html_content: str, vehicle_id: str):
    """Parse HTML table into list of records."""
    try:
        # Extract date
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', html_content)
        data_date = date_match.group(1) if date_match else None
        
        # Find all tables
        table_pattern = r'<table>(.*?)</table>'
        tables = re.findall(table_pattern, html_content, re.DOTALL)
        
        all_records = []
        
        for table in tables:
            # Get trip ID from the h2 before this table
            trip_match = re.search(r'PDX_TRIP\s+(\d+)', html_content.split('<table>')[0])
            trip_id = trip_match.group(1) if trip_match else None
            
            # Find header row
            header_match = re.search(r'<tr>\s*<th>(.*?)</tr>', table, re.DOTALL)
            if not header_match:
                continue
                
            # Extract headers
            headers = re.findall(r'<th>(.*?)</th>', header_match.group(0))
            
            # Find data rows
            data_rows = re.findall(r'<tr>\s*<td>(.*?)</tr>', table, re.DOTALL)
            
            for row in data_rows:
                cells = re.findall(r'<td>(.*?)</td>', '<td>' + row)
                
                if len(cells) == len(headers):
                    record = dict(zip(headers, cells))
                    record['trip_id'] = trip_id
                    record['data_date'] = data_date
                    record['vehicle_id'] = vehicle_id
                    all_records.append(record)
        
        logger.info(f"Parsed {len(all_records)} records for vehicle {vehicle_id}")
        return all_records
        
    except Exception as e:
        logger.error(f"Error parsing HTML for vehicle {vehicle_id}: {e}")
        return []

def validate_stop_event_record(record):
    """Check if record has required fields."""
    required = ['vehicle_number', 'location_id']
    return all(record.get(field) for field in required)

def format_stop_event_for_output(record):
    """Clean up record for output."""
    # Convert numeric fields
    numeric_fields = ['ons', 'offs', 'dwell', 'estimated_load']
    for field in numeric_fields:
        try:
            if field in record:
                record[field] = int(record[field])
        except:
            record[field] = 0
    
    return record