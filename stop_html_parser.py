#!/usr/bin/env python3
"""
HTML parsing module for TriMet stop events data.
Handles parsing of HTML table format returned by the stop events API.
"""

import re
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger('html_parser')

def parse_stop_events_html(html_content: str, vehicle_id: str) -> List[Dict[str, Any]]:
    """
    Parse HTML table format returned by stop events API into structured data.
    
    Expected format:
    Trimet CAD/AVL stop data for YYYY-MM-DD
    Stop events for PDX_TRIP XXXXXXXXX
    vehicle_numberleave_timetrainroute_numberdirectionservice_keytrip_numberstop_timearrive_timedwelllocation_iddoorliftonsoffsestimated_loadmaximum_speedtrain_mileagepattern_distancelocation_distancex_coordinatey_coordinatedata_sourceschedule_status
    [data rows...]
    """
    try:
        lines = html_content.strip().split('\n')
        
        if len(lines) < 3:
            logger.warning(f"Insufficient lines in stop events data for vehicle {vehicle_id}")
            return []
        
        # Extract date from first line: "Trimet CAD/AVL stop data for 2023-01-25"
        date_match = re.search(r'stop data for (\d{4}-\d{2}-\d{2})', lines[0])
        data_date = date_match.group(1) if date_match else None
        
        # Extract trip ID from second line: "Stop events for PDX_TRIP 244851496"
        trip_match = re.search(r'PDX_TRIP (\d+)', lines[1])
        trip_id = trip_match.group(1) if trip_match else None
        
        # Third line contains headers (we'll use predefined column names)
        headers = [
            'vehicle_number', 'leave_time', 'train', 'route_number', 'direction',
            'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell',
            'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
            'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance',
            'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'
        ]
        
        parsed_records = []
        
        # Process data rows (starting from line 3)
        for i, line in enumerate(lines[3:], start=4):
            if not line.strip():
                continue
                
            try:
                # Parse the concatenated data row
                record = parse_stop_event_row(line.strip(), headers)
                
                if record:
                    # Add metadata
                    record.update({
                        'data_date': data_date,
                        'trip_id': trip_id,
                        'vehicle_id': vehicle_id,
                        'row_number': i - 3
                    })
                    parsed_records.append(record)
                    
            except Exception as e:
                logger.warning(f"Error parsing stop event row {i} for vehicle {vehicle_id}: {e}")
                continue
        
        logger.info(f"Parsed {len(parsed_records)} stop events from HTML for vehicle {vehicle_id}")
        return parsed_records
        
    except Exception as e:
        logger.error(f"Error parsing stop events HTML for vehicle {vehicle_id}: {e}")
        return []

def parse_stop_event_row(row_data: str, headers: List[str]) -> Optional[Dict[str, Any]]:
    """
    Parse a single row of stop event data.
    
    The data appears to be concatenated without delimiters. We need to parse it based on
    expected field lengths and patterns.
    
    Example row: 2916234844467440W1010234002348401009520101118.45007623136.46713733.1425
    """
    try:
        # This is a complex parsing task since the data is concatenated
        # We'll use patterns and expected field lengths to extract data
        
        record = {}
        pos = 0
        
        # vehicle_number (4 digits)
        if len(row_data) >= pos + 4:
            record['vehicle_number'] = row_data[pos:pos+4]
            pos += 4
        
        # leave_time (6 digits - HHMMSS)
        if len(row_data) >= pos + 6:
            leave_time_str = row_data[pos:pos+6]
            record['leave_time'] = f"{leave_time_str[:2]}:{leave_time_str[2:4]}:{leave_time_str[4:6]}"
            pos += 6
        
        # train (1 digit)
        if len(row_data) >= pos + 1:
            record['train'] = row_data[pos:pos+1]
            pos += 1
        
        # route_number (4 digits)
        if len(row_data) >= pos + 4:
            record['route_number'] = row_data[pos:pos+4]
            pos += 4
        
        # direction (1 character)
        if len(row_data) >= pos + 1:
            record['direction'] = row_data[pos:pos+1]
            pos += 1
        
        # service_key (1 digit)
        if len(row_data) >= pos + 1:
            record['service_key'] = row_data[pos:pos+1]
            pos += 1
        
        # trip_number (2 digits)
        if len(row_data) >= pos + 2:
            record['trip_number'] = row_data[pos:pos+2]
            pos += 2
        
        # stop_time (6 digits - HHMMSS)
        if len(row_data) >= pos + 6:
            stop_time_str = row_data[pos:pos+6]
            record['stop_time'] = f"{stop_time_str[:2]}:{stop_time_str[2:4]}:{stop_time_str[4:6]}"
            pos += 6
        
        # arrive_time (6 digits - HHMMSS)  
        if len(row_data) >= pos + 6:
            arrive_time_str = row_data[pos:pos+6]
            record['arrive_time'] = f"{arrive_time_str[:2]}:{arrive_time_str[2:4]}:{arrive_time_str[4:6]}"
            pos += 6
        
        # dwell (3 digits)
        if len(row_data) >= pos + 3:
            record['dwell'] = int(row_data[pos:pos+3])
            pos += 3
        
        # location_id (5 digits)
        if len(row_data) >= pos + 5:
            record['location_id'] = row_data[pos:pos+5]
            pos += 5
        
        # door (1 digit)
        if len(row_data) >= pos + 1:
            record['door'] = row_data[pos:pos+1]
            pos += 1
        
        # lift (1 digit)
        if len(row_data) >= pos + 1:
            record['lift'] = row_data[pos:pos+1]
            pos += 1
        
        # ons (2 digits)
        if len(row_data) >= pos + 2:
            record['ons'] = int(row_data[pos:pos+2])
            pos += 2
        
        # offs (2 digits)
        if len(row_data) >= pos + 2:
            record['offs'] = int(row_data[pos:pos+2])
            pos += 2
        
        # estimated_load (2 digits)
        if len(row_data) >= pos + 2:
            record['estimated_load'] = int(row_data[pos:pos+2])
            pos += 2
        
        # maximum_speed (2 digits)
        if len(row_data) >= pos + 2:
            record['maximum_speed'] = int(row_data[pos:pos+2])
            pos += 2
        
        # Remaining fields are floating point numbers with varying precision
        # We'll extract them using regex patterns for decimal numbers
        remaining_data = row_data[pos:]
        
        # Extract floating point numbers from the remaining string
        float_pattern = r'-?\d+\.?\d*'
        float_matches = re.findall(float_pattern, remaining_data)
        
        # Map remaining fields
        float_fields = ['train_mileage', 'pattern_distance', 'location_distance', 
                       'x_coordinate', 'y_coordinate']
        
        for i, field in enumerate(float_fields):
            if i < len(float_matches):
                try:
                    record[field] = float(float_matches[i])
                except ValueError:
                    record[field] = None
            else:
                record[field] = None
        
        # Add remaining fields with default values
        record['data_source'] = 'CAD/AVL'
        record['schedule_status'] = 'unknown'
        
        return record
        
    except Exception as e:
        logger.error(f"Error parsing stop event row: {e}")
        return None

def validate_stop_event_record(record: Dict[str, Any]) -> bool:
    """
    Validate a parsed stop event record.
    
    Args:
        record: Parsed stop event record
        
    Returns:
        True if record is valid, False otherwise
    """
    required_fields = ['vehicle_number', 'leave_time', 'stop_time', 'location_id']
    
    for field in required_fields:
        if field not in record or record[field] is None:
            logger.warning(f"Missing required field: {field}")
            return False
    
    # Validate time formats
    time_fields = ['leave_time', 'stop_time', 'arrive_time']
    time_pattern = r'^\d{2}:\d{2}:\d{2}$'
    
    for field in time_fields:
        if field in record and record[field]:
            if not re.match(time_pattern, str(record[field])):
                logger.warning(f"Invalid time format for {field}: {record[field]}")
                return False
    
    return True

def format_stop_event_for_output(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a stop event record for consistent output.
    
    Args:
        record: Raw parsed record
        
    Returns:
        Formatted record with consistent data types
    """
    formatted = record.copy()
    
    # Ensure numeric fields are properly typed
    numeric_fields = ['dwell', 'ons', 'offs', 'estimated_load', 'maximum_speed']
    for field in numeric_fields:
        if field in formatted and formatted[field] is not None:
            try:
                formatted[field] = int(formatted[field])
            except (ValueError, TypeError):
                formatted[field] = 0
    
    # Ensure float fields are properly typed
    float_fields = ['train_mileage', 'pattern_distance', 'location_distance', 
                   'x_coordinate', 'y_coordinate']
    for field in float_fields:
        if field in formatted and formatted[field] is not None:
            try:
                formatted[field] = float(formatted[field])
            except (ValueError, TypeError):
                formatted[field] = 0.0
    
    return formatted