#!/usr/bin/env python3
"""
Data transformation script for TriMet bus breadcrumb and stop event data.
This script processes JSONL files containing either breadcrumb or stop event data
and loads them into PostgreSQL.
"""

import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import os
import logging
import sys

# Database connection parameters
db_params = {
    'database': os.getenv('DB_NAME', 'busdata'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Cloud410!'),
    'host': os.getenv('DB_HOST', '10.116.4.3'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# Function to detect data type from the first valid record
def detect_data_type(file_path, logger):
    """Detect whether the file contains breadcrumb or stop event data."""
    try:
        with open(file_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    if 'data_type' in data:
                        return data['data_type']
                    # Fall back to field detection
                    elif 'GPS_LATITUDE' in data and 'GPS_LONGITUDE' in data:
                        return 'breadcrumbs'
                    elif 'location_id' in data and 'stop_time' in data:
                        return 'stopevents'
                except json.JSONDecodeError:
                    continue
        
        logger.warning(f"Could not detect data type from {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error detecting data type: {str(e)}")
        return None

# Function to parse OPD_DATE and ACT_TIME into a timestamp (for breadcrumbs)
def parse_timestamp(opd_date, act_time, logger):
    try:
        # Parse the date part (e.g., "25DEC2022:00:00:00")
        date_str = opd_date.split(':')[0]  # "25DEC2022"
        day = int(date_str[:2])
        month_str = date_str[2:5]
        year = int(date_str[5:])
        
        # Convert month string to number
        months = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        month = months[month_str]
        
        # Calculate hours, minutes, seconds from ACT_TIME (seconds since midnight)
        # Handle cases where ACT_TIME exceeds 24 hours (86400 seconds)
        days_to_add = act_time // 86400  # Number of days to add
        seconds_in_day = act_time % 86400  # Remaining seconds in the day
        
        hours = seconds_in_day // 3600
        minutes = (seconds_in_day % 3600) // 60
        seconds = seconds_in_day % 60
        
        # Create base datetime object
        base_date = datetime(year, month, day, hours, minutes, seconds)
        
        # Add any additional days if ACT_TIME exceeded 24 hours
        if days_to_add > 0:
            base_date = base_date + timedelta(days=days_to_add)
            
        return base_date
    except Exception as e:
        logger.error(f"Error parsing timestamp: {opd_date}, {act_time} - {str(e)}")
        return None

# Function to parse stop event timestamp
def parse_stop_timestamp(data_date, time_seconds, logger):
    """Parse stop event timestamp from data_date and time in seconds."""
    try:
        # Parse the date (e.g., "2023-01-26")
        date_obj = datetime.strptime(data_date, '%Y-%m-%d')
        
        # Handle cases where time exceeds 24 hours (86400 seconds)
        days_to_add = time_seconds // 86400
        seconds_in_day = time_seconds % 86400
        
        hours = seconds_in_day // 3600
        minutes = (seconds_in_day % 3600) // 60
        seconds = seconds_in_day % 60
        
        # Create timestamp
        timestamp = date_obj.replace(hour=hours, minute=minutes, second=seconds)
        
        # Add any additional days if time exceeded 24 hours
        if days_to_add > 0:
            timestamp = timestamp + timedelta(days=days_to_add)
            
        return timestamp
    except Exception as e:
        logger.error(f"Error parsing stop timestamp: {data_date}, {time_seconds} - {str(e)}")
        return None

# Function to convert service key
def convert_service_key(service_key_raw, date_obj=None):
    """Convert service key to our enum format."""
    # Handle different service key formats
    if isinstance(service_key_raw, str):
        if service_key_raw.upper() in ['W', 'WEEKDAY']:
            return 'Weekday'
        elif service_key_raw.upper() in ['S', 'SATURDAY']:
            return 'Saturday'
        elif service_key_raw.upper() in ['U', 'SUNDAY']:
            return 'Sunday'
    
    # Fall back to date-based detection if available
    if date_obj:
        weekday = date_obj.weekday()
        if weekday == 5:
            return 'Saturday'
        elif weekday == 6:
            return 'Sunday'
        else:
            return 'Weekday'
    
    # Default fallback
    return 'Weekday'

# Function to convert direction
def convert_direction(direction_raw):
    """Convert direction to our enum format."""
    if str(direction_raw) == '0':
        return 'Out'
    elif str(direction_raw) == '1':
        return 'Back'
    else:
        return 'Out'  # Default

# Function to remove existing data for a specific date
def remove_existing_data(date_str, conn, cursor, logger):
    try:
        # Start a subtransaction
        cursor.execute("SAVEPOINT before_delete")
        
        # Delete existing breadcrumbs for the date
        cursor.execute("""
            DELETE FROM BreadCrumb
            WHERE DATE(tstamp) = %s
        """, (date_str,))
        
        affected_rows = cursor.rowcount
        logger.info(f"Removed {affected_rows} existing breadcrumbs for {date_str}")
        
        # Commit the subtransaction
        cursor.execute("RELEASE SAVEPOINT before_delete")
        return True
    except Exception as e:
        # Rollback to the savepoint
        cursor.execute("ROLLBACK TO SAVEPOINT before_delete")
        logger.error(f"Error removing existing data for {date_str}: {str(e)}")
        return False

# Function to process breadcrumb data (original logic)
def process_breadcrumb_data(breadcrumbs, logger):
    """Process breadcrumb data and return trips_data and breadcrumb_data."""
    # Sort breadcrumbs by trip_id and timestamp
    breadcrumbs.sort(key=lambda x: (x['EVENT_NO_TRIP'], x['ACT_TIME']))
    logger.info(f"Sorted breadcrumbs by trip_id and ACT_TIME")
    
    # Group by trip_id for processing
    trips_data = {}
    breadcrumbs_by_trip = {}
    
    for bc in breadcrumbs:
        trip_id = bc['EVENT_NO_TRIP']
        if trip_id not in breadcrumbs_by_trip:
            breadcrumbs_by_trip[trip_id] = []
        breadcrumbs_by_trip[trip_id].append(bc)
        
        # Store trip data (only once per trip)
        if trip_id not in trips_data:
            # Determine service_key based on the day of the week from OPD_DATE
            date_obj = bc['OPD_DATE'].split(':')[0]
            day = int(date_obj[:2])
            month_str = date_obj[2:5]
            year = int(date_obj[5:])
            
            months = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
            }
            month = months[month_str]
            
            # Create a date object to determine day of week
            date_obj = datetime(year, month, day)
            service_key = convert_service_key(None, date_obj)
            direction = 'Out'  # Default direction
            
            trips_data[trip_id] = (
                trip_id,              # trip_id
                None,                 # route_id (to be populated later)
                bc['VEHICLE_ID'],     # vehicle_id
                service_key,          # service_key
                direction             # direction
            )
    
    logger.info(f"Identified {len(trips_data)} unique trips from breadcrumbs")
    
    # Process breadcrumbs and calculate speeds
    breadcrumb_data = []
    
    for trip_id, trip_breadcrumbs in breadcrumbs_by_trip.items():
        second_breadcrumb_speed = None
        
        # Process each breadcrumb in the trip
        for i, bc in enumerate(trip_breadcrumbs):
            timestamp = parse_timestamp(bc['OPD_DATE'], bc['ACT_TIME'], logger)
            if timestamp is None:
                logger.warning(f"Skipping breadcrumb due to timestamp parsing error: {bc}")
                continue
                
            speed = None
            
            # Calculate speed if not the first breadcrumb
            if i > 0:
                prev_bc = trip_breadcrumbs[i-1]
                meters_diff = bc['METERS'] - prev_bc['METERS']
                time_diff = bc['ACT_TIME'] - prev_bc['ACT_TIME']
                
                if time_diff > 0:  # Avoid division by zero
                    speed = meters_diff / time_diff  # meters per second
                    
                    # Store speed of second breadcrumb
                    if i == 1:
                        second_breadcrumb_speed = speed
            
            # For first breadcrumb, use speed of second breadcrumb (as per assignment)
            if i == 0 and len(trip_breadcrumbs) > 1:
                # We'll update this after processing the second breadcrumb
                breadcrumb_data.append((
                    timestamp,
                    bc['GPS_LATITUDE'],
                    bc['GPS_LONGITUDE'],
                    None,  # Placeholder for speed
                    trip_id
                ))
            else:
                breadcrumb_data.append((
                    timestamp,
                    bc['GPS_LATITUDE'],
                    bc['GPS_LONGITUDE'],
                    speed,
                    trip_id
                ))
        
        # Update first breadcrumb with second breadcrumb's speed if available
        if second_breadcrumb_speed is not None and len(trip_breadcrumbs) > 1:
            # Find the first breadcrumb for this trip in breadcrumb_data
            for idx, (ts, lat, lon, spd, t_id) in enumerate(breadcrumb_data):
                if t_id == trip_id and spd is None:
                    breadcrumb_data[idx] = (ts, lat, lon, second_breadcrumb_speed, t_id)
                    break
    
    return trips_data, breadcrumb_data

# Function to process stop event data
def process_stop_event_data(stop_events, logger):
    """Process stop event data and return trips_data and stop_data."""
    trips_data = {}
    stop_data = []
    
    for se in stop_events:
        trip_id = int(se['trip_id'])
        
        # Store trip data (only once per trip)
        if trip_id not in trips_data:
            # Parse date for service key determination
            date_obj = datetime.strptime(se['data_date'], '%Y-%m-%d')
            service_key = convert_service_key(se.get('service_key'), date_obj)
            direction = convert_direction(se.get('direction', '0'))
            
            trips_data[trip_id] = (
                trip_id,                          # trip_id
                int(se['route_number']),          # route_id
                int(se['vehicle_id']),            # vehicle_id
                service_key,                      # service_key
                direction                         # direction
            )
        
        # Process stop event timestamps
        arrive_time = parse_stop_timestamp(se['data_date'], int(se['arrive_time']), logger)
        if arrive_time:
            # Convert coordinates (assuming they're in state plane coordinates)
            # For now, we'll use them as lat/lon placeholders
            # In a real implementation, you'd convert from state plane to lat/lon
            x_coord = float(se['x_coordinate'])
            y_coord = float(se['y_coordinate'])
            
            # Rough conversion (this is a placeholder - use proper projection)
            # You should use a proper coordinate transformation library like pyproj
            longitude = x_coord / 100000  # Placeholder conversion
            latitude = y_coord / 100000   # Placeholder conversion
            
            # Use maximum_speed as the speed (convert from string to float)
            try:
                speed = float(se['maximum_speed']) if se['maximum_speed'] else None
            except (ValueError, TypeError):
                speed = None
            
            stop_data.append((
                arrive_time,
                latitude,
                longitude,
                speed,
                trip_id
            ))
    
    logger.info(f"Identified {len(trips_data)} unique trips from stop events")
    return trips_data, stop_data

# Function to process a specific JSONL file
def process_day_file(date_str, logger, clear_existing=True):
    # Construct file path based on the date
    file_path = f"/opt/busdata/output/gg.jsonl"
    
    # Check if file exists
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return
    
    logger.info(f"Processing file: {file_path}")
    
    # Detect data type
    data_type = detect_data_type(file_path, logger)
    if data_type is None:
        logger.error(f"Could not determine data type for {file_path}")
        return
    
    logger.info(f"Detected data type: {data_type}")
    
    # Connect to the database
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_session(autocommit=False)
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL database")
        
        # Clear existing data if requested
        if clear_existing:
            success = remove_existing_data(date_str, conn, cursor, logger)
            if not success:
                logger.warning("Skipping file processing due to error in removing existing data")
                return
        
        # Read JSONL file
        records = []
        line_count = 0
        with open(file_path, 'r') as f:
            for line in f:
                line_count += 1
                try:
                    record = json.loads(line.strip())
                    records.append(record)
                except json.JSONDecodeError:
                    logger.warning(f"Error decoding JSON at line {line_count} in {file_path}")
                    continue
        
        logger.info(f"Read {len(records)} valid records from {line_count} lines")
        
        if not records:
            logger.warning(f"No records found in {file_path}")
            return
        
        # Process data based on type
        if data_type == 'breadcrumbs':
            trips_data, breadcrumb_data = process_breadcrumb_data(records, logger)
            data_to_insert = breadcrumb_data
        elif data_type == 'stopevents':
            trips_data, stop_data = process_stop_event_data(records, logger)
            data_to_insert = stop_data
        else:
            logger.error(f"Unknown data type: {data_type}")
            return
        
        # Insert trips into Trip table
        if trips_data:
            try:
                cursor.execute("SAVEPOINT before_trip_insert")
                
                trip_insert_query = '''
                    INSERT INTO Trip (trip_id, route_id, vehicle_id, service_key, direction)
                    VALUES %s
                    ON CONFLICT (trip_id) DO NOTHING
                '''
                execute_values(cursor, trip_insert_query, list(trips_data.values()))
                logger.info(f"Inserted trip data into Trip table")
                
                cursor.execute("RELEASE SAVEPOINT before_trip_insert")
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT before_trip_insert")
                logger.error(f"Error inserting trips: {str(e)}")
        
        # Insert data into BreadCrumb table (works for both breadcrumbs and stop events)
        if data_to_insert:
            breadcrumb_insert_query = '''
                INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES %s
                ON CONFLICT DO NOTHING
            '''
            # Insert in batches of 1000 to avoid memory issues
            batch_size = 1000
            total_inserted = 0
            errors = 0
            
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i+batch_size]
                try:
                    cursor.execute("SAVEPOINT before_batch_insert")
                    execute_values(cursor, breadcrumb_insert_query, batch)
                    rows_inserted = cursor.rowcount
                    total_inserted += rows_inserted
                    cursor.execute("RELEASE SAVEPOINT before_batch_insert")
                    logger.info(f"Inserted batch of {rows_inserted} records ({total_inserted}/{len(data_to_insert)})")
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT before_batch_insert")
                    errors += 1
                    logger.error(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
        
        # Run a query to verify data was inserted
        cursor.execute("SELECT COUNT(*) FROM BreadCrumb WHERE DATE(tstamp) = %s", (date_str,))
        count = cursor.fetchone()[0]
        logger.info(f"Total records in database for {date_str}: {count}")
        
        if errors > 0:
            logger.warning(f"Completed with {errors} batch errors - some data may be missing")
            
        # Commit changes
        conn.commit()
        logger.info(f"Successfully committed data to database")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
    finally:
        # Close connection
        if conn:
            if cursor:
                cursor.close()
            conn.close()
            logger.info("Database connection closed")

def main(date_str=None, logger=None, clear_existing=True):
    """Main function to process a day's data."""
    # Use provided logger or create a new one
    if logger is None:
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("/opt/busdata/data_transformer.log"),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger('data_transformer')
    
    logger.info("Starting data transformer...")
    
    # If no date is provided, use yesterday's date
    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Processing data for date: {date_str}")
    process_day_file(date_str, logger, clear_existing)
    logger.info("Data transformation completed")

if __name__ == "__main__":
    try:
        # Parse command-line arguments
        import argparse
        parser = argparse.ArgumentParser(description='Process TriMet breadcrumb and stop event data.')
        parser.add_argument('date', nargs='?', help='Date to process (YYYY-MM-DD format)')
        parser.add_argument('--no-clear', action='store_true', help='Do not clear existing data for the date')
        args = parser.parse_args()
        
        # Set date (use provided or yesterday)
        if args.date:
            date_str = args.date
            # Validate date format
            datetime.strptime(date_str, '%Y-%m-%d')
        else:
            # Use yesterday's date by default
            date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        clear_existing = not args.no_clear
        
        main(date_str, clear_existing=clear_existing)
    except ValueError:
        print("Error: Date must be in format YYYY-MM-DD")
        sys.exit(1)
    except Exception as e:
        print(f"Unhandled exception in main function: {str(e)}")
        sys.exit(1)