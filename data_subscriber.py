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
# LOG_DIR = os.environ.get('BUSDATA_LOG_DIR', '/opt/busdata')
# OUTPUT_DIR = os.environ.get('BUSDATA_OUTPUT_DIR', '/opt/busdata/output')

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

# Dictionary to store daily files
daily_files = {}

def get_daily_file(date_str):
    """Get or create a file handle for the specified date."""
    if date_str not in daily_files:
        filename = os.path.join(OUTPUT_DIR, f"{OUTPUT_PREF}_{date_str}.jsonl")
        daily_files[date_str] = open(filename, 'a')
        logger.info(f"Created/opened file for {date_str}: {filename}")
    
    return daily_files[date_str]

def close_old_files(current_date_str):
    """Close file handles for dates other than the current date."""
    for date_str, file_handle in list(daily_files.items()):
        if date_str != current_date_str:
            file_handle.close()
            del daily_files[date_str]
            logger.info(f"Closed file for {date_str}")

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
        
        # Acknowledge the message
        message.ack()
        
        # Close old files if date has changed
        current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        close_old_files(current_date_str)
        
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        # Negative acknowledgement - the message will be redelivered
        message.nack()

def run_transformation(date_str):
    """Run transformation for the specified date."""
    logger.info(f"Starting transformation for {date_str}...")
    
    try:
        # Import the transform module
        import transform
        
        # Run the transformation
        transform.main(date_str, logger)
        
        logger.info(f"Transformation completed for {date_str}")
    except Exception as e:
        logger.error(f"Error during transformation for {date_str}: {e}", exc_info=True)


def main(subscription_name=None):
    """Main function to receive and process messages from Pub/Sub."""
    # Use provided arguments or fall back to defaults
    sub_name = subscription_name or SUBSCRIPTION_NAME
    
    logger.info(f"Starting subscriber on {sub_name}...")
    logger.info(f"Using project: {PROJECT_ID}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    
    # Initialize Pub/Sub subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, sub_name)
    
    # Configure the subscriber
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=process_message
    )
    
    try:
        # Keep the subscriber running indefinitely
        logger.info(f"Listening for messages on {subscription_path}...")
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logger.info("Subscriber stopped by user.")
    except Exception as e:
        streaming_pull_future.cancel()
        logger.error(f"Subscriber stopped due to error: {e}", exc_info=True)
    finally:
        # Close all file handles and run transformations
        current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        for date_str, file_handle in daily_files.items():
            file_handle.close()
            logger.info(f"Closed file for {date_str}")
            
            # Run transformation for each date
            run_transformation(date_str)
        
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
  python3 data_subscriber.py
  python3 data_subscriber.py --subscription breadcrumb-data-subscription
  python3 data_subscriber.py --subscription stop-data-subscription
        '''
    )
    
    parser.add_argument(
        '--subscription', '-s',
        default=None,
        help=f'Pub/Sub subscription name (default: {DEFAULT_SUBSCRIPTION})'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    try:
        main(subscription_name=args.subscription)
    except Exception as e:
        logger.critical(f"Unhandled exception in main function: {e}", exc_info=True)
        
        # Run transformation for current date as fallback
        current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        run_transformation(current_date_str)