#!/usr/bin/env python3
"""
Data subscriber script for TriMet bus breadcrunb/stop data.
This script receives breadcrunb/stop records from a Google Cloud Pub/Sub subscription
and saves them to daily files.
"""

import os
import json
import time
import datetime
import logging
import argparse
from google.cloud import pubsub_v1

# Get configuration from environment variables or use defaults
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', 'dataeng-456707')
DEFAULT_SUBSCRIPTION = os.environ.get('BUSDATA_SUBSCRIPTION', 'breadcrumb-data-subscription')
OUTPUT_FILE_PRFIX = os.environ.get('GOOGLE_CLOUD_PROJECT', 'dataeng-456707')

# Import configuration if available
try:
    import subscriber_config
    SUBSCRIPTION_NAME = getattr(subscriber_config, 'SUBSCRIPTION_NAME', DEFAULT_SUBSCRIPTION)
    PROJECT_ID = getattr(subscriber_config, 'PROJECT_ID', PROJECT_ID)
except ImportError:
    SUBSCRIPTION_NAME = DEFAULT_SUBSCRIPTION

# Get paths from environment variables with fallbacks
LOG_DIR = os.environ.get('BUSDATA_LOG_DIR', '.')
OUTPUT_DIR = os.environ.get('BUSDATA_OUTPUT_DIR', '.')
OUTPUT_PREF = os.environ.get('OUTPUT_FILE_PREFIX', 'breadcrumbs')

# Ensure directories exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Setup logging with configurable paths
log_file_path = os.path.join(LOG_DIR, 'data_subscriber.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('data_subscriber')

# Log the configuration
logger.info(f"Configuration loaded:")
logger.info(f"  Project ID: {PROJECT_ID}")
logger.info(f"  Default subscription: {DEFAULT_SUBSCRIPTION}")
logger.info(f"  Log directory: {LOG_DIR}")
logger.info(f"  Output directory: {OUTPUT_DIR}")

# Dictionary to store daily files and counters
daily_files = {}
record_counters = {}
last_log_time = {}

def get_daily_file(date_str):
    """Get or create a file handle for the specified date."""
    if date_str not in daily_files:
        filename = os.path.join(OUTPUT_DIR, f"{OUTPUT_PREF}_{date_str}.jsonl")
        daily_files[date_str] = open(filename, 'a')
        record_counters[date_str] = 0
        last_log_time[date_str] = time.time()
        logger.info(f"Created/opened file for {date_str}: {filename}")
    
    return daily_files[date_str]

def close_old_files(current_date_str):
    """Close file handles for dates other than the current date."""
    for date_str, file_handle in list(daily_files.items()):
        if date_str != current_date_str:
            # Log final count before closing
            total_records = record_counters.get(date_str, 0)
            logger.info(f"Closing file for {date_str} - Final record count: {total_records}")
            
            file_handle.close()
            del daily_files[date_str]
            del record_counters[date_str]
            if date_str in last_log_time:
                del last_log_time[date_str]

def log_progress(date_str):
    """Log progress for file writing with throttling."""
    current_time = time.time()
    count = record_counters.get(date_str, 0)
    
    # Log every 100 records or every 30 seconds
    should_log = (
        count % 100 == 0 or 
        (current_time - last_log_time.get(date_str, 0)) >= 30
    )
    
    if should_log:
        filename = f"{OUTPUT_PREF}_{date_str}.jsonl"
        logger.info(f"Writing to {filename} - Records written: {count}")
        last_log_time[date_str] = current_time

def process_message(message):
    """Process a Pub/Sub message and write to the appropriate daily file."""
    try:
        # Parse the message data
        data = json.loads(message.data.decode("utf-8"))
        
        # Extract timestamp from the data (assuming it exists)
        # If timestamp is not available, use the current date
        if "timestamp" in data:
            # Parse the timestamp to get the date
            # Assuming timestamp format is like "2025-04-13 14:30:45"
            date_str = data["timestamp"].split()[0]
        else:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Get the file handle for this date
        file_handle = get_daily_file(date_str)
        
        # Write the record to the file (as a JSON line)
        file_handle.write(json.dumps(data) + "\n")
        file_handle.flush()  # Ensure data is written immediately
        
        # Update counter and log progress
        record_counters[date_str] = record_counters.get(date_str, 0) + 1
        log_progress(date_str)
        
        # Acknowledge the message
        message.ack()
        
        # Close old files if date has changed
        current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        close_old_files(current_date_str)
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message: {e}")
        logger.error(f"Message data: {message.data.decode('utf-8', errors='replace')[:200]}...")
        message.nack()
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        message.nack()

def log_periodic_stats():
    """Log periodic statistics about all active files."""
    if daily_files:
        logger.info("=== Current File Statistics ===")
        total_records = 0
        for date_str in daily_files.keys():
            count = record_counters.get(date_str, 0)
            filename = f"{OUTPUT_PREF}_{date_str}.jsonl"
            logger.info(f"  {filename}: {count} records")
            total_records += count
        logger.info(f"  Total records: {total_records}")
        logger.info("===============================")

def main(subscription_name=None, data_type=None):
    """Main function to receive and process messages from Pub/Sub."""
    global OUTPUT_PREF
    
    # Set output prefix based on data type argument
    if data_type:
        OUTPUT_PREF = data_type
        logger.info(f"Output prefix set to: {OUTPUT_PREF}")
    
    # Use provided arguments or fall back to defaults
    sub_name = subscription_name or SUBSCRIPTION_NAME
    
    logger.info(f"Starting subscriber on {sub_name}...")
    logger.info(f"Using project: {PROJECT_ID}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Output file prefix: {OUTPUT_PREF}")
    
    # Initialize Pub/Sub subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, sub_name)
    
    # Configure the subscriber
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=process_message
    )
    
    # Set up periodic logging
    last_stats_log = time.time()
    stats_interval = 300  # Log stats every 5 minutes
    
    try:
        # Keep the subscriber running indefinitely
        logger.info(f"Listening for messages on {subscription_path}...")
        logger.info(f"Files will be saved as: {OUTPUT_PREF}_YYYY-MM-DD.jsonl")
        logger.info("Progress will be logged every 100 records or 30 seconds")
        
        while True:
            try:
                # Non-blocking check with timeout
                streaming_pull_future.result(timeout=60)
                break
            except TimeoutError:
                # Log periodic stats
                current_time = time.time()
                if current_time - last_stats_log >= stats_interval:
                    log_periodic_stats()
                    last_stats_log = current_time
                continue
                
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logger.info("Subscriber stopped by user.")
        log_periodic_stats()
        
    except Exception as e:
        streaming_pull_future.cancel()
        logger.error(f"Subscriber stopped due to error: {e}", exc_info=True)
    finally:
        # Close all file handles with final counts
        logger.info("=== Final File Statistics ===")
        for date_str, file_handle in daily_files.items():
            count = record_counters.get(date_str, 0)
            filename = f"{OUTPUT_PREF}_{date_str}.jsonl"
            logger.info(f"Closing {filename} - Final count: {count} records")
            file_handle.close()
        logger.info("=============================")
        
        # Close the subscriber client
        subscriber.close()
        logger.info("Subscriber client closed.")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='TriMet Bus Data Subscriber',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python3 data_subscriber.py --data-type breadcrumbs
  python3 data_subscriber.py --data-type stops --subscription stop-data-subscription
  python3 data_subscriber.py --data-type vehicles --subscription vehicle-data-subscription
        '''
    )
    
    parser.add_argument(
        '--subscription', '-s',
        default=None,
        help=f'Pub/Sub subscription name (default: {DEFAULT_SUBSCRIPTION})'
    )
    
    parser.add_argument(
        '--data-type', '-t',
        default=None,
        help='Data type for output file prefix (e.g., breadcrumbs, stops, vehicles, trips)'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    try:
        main(subscription_name=args.subscription, data_type=args.data_type)
    except Exception as e:
        logger.critical(f"Unhandled exception in main function: {e}", exc_info=True)
        
        # Log final stats even on error
        if daily_files:
            logger.info("=== Emergency Final Statistics ===")
            for date_str in daily_files.keys():
                count = record_counters.get(date_str, 0)
                logger.info(f"File {OUTPUT_PREF}_{date_str}.jsonl: {count} records")