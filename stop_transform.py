#!/usr/bin/env python3
"""
Route updater script - reads stop events JSONL and updates Trip table with route numbers.
Similar to breadcrumb transform but focused only on updating route_id in existing Trip records.
"""

import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import os
import logging
import sys
from collections import defaultdict

# Database connection parameters
db_params = {
    'database': os.getenv('DB_NAME', 'busdata'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Cloud410!'),
    'host': os.getenv('DB_HOST', '10.116.4.3'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

def process_stop_events_file(file_path, logger):
    """Process stop events JSONL file and extract trip-route mappings."""
    
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return
    
    logger.info(f"Processing stop events file: {file_path}")
    
    # Dictionary to store trip_id -> route_number mappings
    # route_number from JSONL will become route_id in database
    # Use dict to automatically eliminate duplicates
    trip_route_mappings = {}
    
    # Read JSONL file
    line_count = 0
    valid_records = 0
    
    with open(file_path, 'r') as f:
        for line in f:
            line_count += 1
            try:
                stop_event = json.loads(line.strip())
                
                # Extract trip_id and route_number
                trip_id = stop_event.get('trip_id')
                route_number = stop_event.get('route_number')
                
                if trip_id and route_number:
                    try:
                        trip_id_int = int(trip_id)
                        route_number_int = int(route_number)
                        
                        # Store mapping: trip_id -> route_number
                        # route_number will become route_id in the database
                        trip_route_mappings[trip_id_int] = route_number_int
                        valid_records += 1
                        
                    except ValueError:
                        logger.warning(f"Invalid trip_id or route_number at line {line_count}: {trip_id}, {route_number}")
                else:
                    logger.debug(f"Missing trip_id or route_number at line {line_count}")
                    
            except json.JSONDecodeError:
                logger.warning(f"Error decoding JSON at line {line_count}")
                continue
    
    logger.info(f"Read {line_count} lines, found {valid_records} valid stop events")
    logger.info(f"Extracted {len(trip_route_mappings)} unique trip-route mappings")
    
    return trip_route_mappings

def update_trip_routes(trip_route_mappings, logger):
    """Update Trip table with route IDs."""
    
    if not trip_route_mappings:
        logger.warning("No trip-route mappings to update")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_session(autocommit=False)
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL database")
        
        # Prepare update data as list of tuples (route_number, trip_id)
        # route_number from JSONL will become route_id in the Trip table
        update_data = [(route_number, trip_id) for trip_id, route_number in trip_route_mappings.items()]
        
        logger.info(f"Updating {len(update_data)} trip records with route information")
        
        # Update in batches
        batch_size = 1000
        total_updated = 0
        errors = 0
        
        # SQL for updating route_id where trip_id matches
        # Note: route_number from JSONL becomes route_id in database
        update_query = """
            UPDATE Trip 
            SET route_id = data.route_number 
            FROM (VALUES %s) AS data(route_number, trip_id) 
            WHERE Trip.trip_id = data.trip_id
        """
        
        for i in range(0, len(update_data), batch_size):
            batch = update_data[i:i+batch_size]
            try:
                cursor.execute("SAVEPOINT before_update_batch")
                
                # Use execute_values for batch update
                execute_values(
                    cursor, 
                    update_query, 
                    batch,
                    template=None,
                    page_size=1000
                )
                
                rows_updated = cursor.rowcount
                total_updated += rows_updated
                
                cursor.execute("RELEASE SAVEPOINT before_update_batch")
                logger.info(f"Updated batch {i//batch_size + 1}: {rows_updated} rows ({total_updated} total)")
                
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT before_update_batch")
                errors += 1
                logger.error(f"Error updating batch {i//batch_size + 1}: {str(e)}")
                continue
        
        # Verify updates
        cursor.execute("SELECT COUNT(*) FROM Trip WHERE route_id IS NOT NULL")
        trips_with_routes = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Trip WHERE route_id IS NULL")
        trips_without_routes = cursor.fetchone()[0]
        
        logger.info(f"Update summary:")
        logger.info(f"  - Total updates attempted: {len(update_data)}")
        logger.info(f"  - Rows actually updated: {total_updated}")
        logger.info(f"  - Batch errors: {errors}")
        logger.info(f"  - Trips with route_id: {trips_with_routes}")
        logger.info(f"  - Trips without route_id: {trips_without_routes}")
        
        # Commit changes
        conn.commit()
        logger.info("Successfully committed route updates to database")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error updating trip routes: {str(e)}", exc_info=True)
    finally:
        if conn:
            if cursor:
                cursor.close()
            conn.close()
            logger.info("Database connection closed")

def main(file_path=None, logger=None):
    """Main function to update trip routes."""
    
    # Setup logging if not provided
    if logger is None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("/opt/busdata/route_updater.log"),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger('route_updater')
    
    logger.info("Starting Trip route updater...")
    
    # Default file path if not provided
    if file_path is None:
        file_path = "/opt/busdata/output/stop_events.jsonl"
    
    # Process the file and extract mappings
    trip_route_mappings = process_stop_events_file(file_path, logger)
    
    if trip_route_mappings:
        # Update the database
        update_trip_routes(trip_route_mappings, logger)
    else:
        logger.warning("No valid trip-route mappings found, nothing to update")
    
    logger.info("Trip route updater completed")

if __name__ == "__main__":
    try:
        import argparse
        parser = argparse.ArgumentParser(description='Update Trip table with route IDs from stop events.')
        parser.add_argument('file', nargs='?', 
                          default="/opt/busdata/output/stop_events.jsonl",
                          help='Path to stop events JSONL file')
        args = parser.parse_args()
        
        main(args.file)
        
    except Exception as e:
        print(f"Unhandled exception in main function: {str(e)}")
        sys.exit(1)