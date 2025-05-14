#!/usr/bin/env python3
"""
Data transformation script for TriMet bus breadcrumb data.
This script processes JSONL breadcrumb files and loads them into PostgreSQL.
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
    'database': 'busdata',
    'user': 'postgres',
    'password': 'Cloud410!',
    'host': '10.116.4.3',
    'port': '5432'
}

# Function to parse OPD_DATE and ACT_TIME into a timestamp
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

# Function to process a specific JSONL file
def process_day_file(date_str, logger, clear_existing=True):
    # Construct file path based on the date
    file_path = f"/opt/busdata/output/breadcrumbs_{date_str}.jsonl"
    
    # Check if file exists
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return
    
    logger.info(f"Processing file: {file_path}")
    
    # Connect to the database
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        # Set isolation level to ensure proper transaction control
        conn.set_session(autocommit=False)
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL database")
        
        # Clear existing data if requested
        if clear_existing:
            success = remove_existing_data(date_str, conn, cursor, logger)
            if not success:
                logger.warning("Skipping file processing due to error in removing existing data")
                return
        
        breadcrumbs = []
        # Read JSONL file (each line is a separate JSON object)
        line_count = 0
        with open(file_path, 'r') as f:
            for line in f:
                line_count += 1
                try:
                    bc = json.loads(line.strip())
                    breadcrumbs.append(bc)
                except json.JSONDecodeError:
                    logger.warning(f"Error decoding JSON at line {line_count} in {file_path}")
                    continue
        
        logger.info(f"Read {len(breadcrumbs)} valid breadcrumbs from {line_count} lines")
        
        if not breadcrumbs:
            logger.warning(f"No breadcrumbs found in {file_path}")
            return
        
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
                weekday = date_obj.weekday()
                
                # 5 = Saturday, 6 = Sunday, 0-4 = Weekday
                if weekday == 5:
                    service_key = 'Saturday'
                elif weekday == 6:
                    service_key = 'Sunday'
                else:
                    service_key = 'Weekday'
                
                # Default direction is 'Out' (equivalent to '0' in the schema)
                direction = 'Out'
                
                trips_data[trip_id] = (
                    trip_id,              # trip_id
                    None,                 # route_id (to be populated later)
                    bc['VEHICLE_ID'],     # vehicle_id
                    service_key,          # service_key
                    direction             # direction
                )
        
        logger.info(f"Identified {len(trips_data)} unique trips")
        
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
                # Continue with breadcrumbs for trips that might already exist
        
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
                breadcrumb_data[0] = (
                    breadcrumb_data[0][0],  # timestamp
                    breadcrumb_data[0][1],  # latitude
                    breadcrumb_data[0][2],  # longitude
                    second_breadcrumb_speed,  # use second breadcrumb's speed
                    breadcrumb_data[0][4]   # trip_id
                )
        
        logger.info(f"Processed {len(breadcrumb_data)} breadcrumbs with calculated speeds")
        
        # Insert breadcrumbs into BreadCrumb table
        if breadcrumb_data:
            breadcrumb_insert_query = '''
                INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES %s
                ON CONFLICT DO NOTHING
            '''
            # Insert in batches of 1000 to avoid memory issues
            batch_size = 1000
            total_inserted = 0
            errors = 0
            
            for i in range(0, len(breadcrumb_data), batch_size):
                batch = breadcrumb_data[i:i+batch_size]
                try:
                    cursor.execute("SAVEPOINT before_batch_insert")
                    execute_values(cursor, breadcrumb_insert_query, batch)
                    rows_inserted = cursor.rowcount
                    total_inserted += rows_inserted
                    cursor.execute("RELEASE SAVEPOINT before_batch_insert")
                    logger.info(f"Inserted batch of {rows_inserted} breadcrumbs ({total_inserted}/{len(breadcrumb_data)})")
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT before_batch_insert")
                    errors += 1
                    logger.error(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
                    # Continue with next batch
        
        # Run a query to verify data was inserted
        cursor.execute("SELECT COUNT(*) FROM BreadCrumb WHERE DATE(tstamp) = %s", (date_str,))
        count = cursor.fetchone()[0]
        logger.info(f"Total breadcrumbs in database for {date_str}: {count}")
        
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
    """Main function to process a day's breadcrumb data."""
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
    
    logger.info("Starting breadcrumb data transformer...")
    
    # If no date is provided, use yesterday's date
    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Processing breadcrumbs for date: {date_str}")
    process_day_file(date_str, logger, clear_existing)
    logger.info("Breadcrumb data transformation completed")

if __name__ == "__main__":
    try:
        # Parse command-line arguments
        import argparse
        parser = argparse.ArgumentParser(description='Process TriMet breadcrumb data.')
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