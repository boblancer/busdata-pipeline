#!/usr/bin/env python3
"""
Data subscriber script for TriMet bus breadcrumb data.
This script receives breadcrumb records from a Google Cloud Pub/Sub subscription
and saves them to daily files.
"""

import os
import json
import time
import datetime
import logging
from google.cloud import pubsub_v1

# Import configuration if available
try:
    import subscriber_config
    PROJECT_ID = subscriber_config.PROJECT_ID
    SUBSCRIPTION_NAME = subscriber_config.SUBSCRIPTION_NAME
except ImportError:
    # Default configuration if config module is not available
    PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "dataeng-456707")
    SUBSCRIPTION_NAME = "breadcrumb-data-subscription"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/opt/busdata/data_subscriber.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('data_subscriber')

# Define output directory
OUTPUT_DIR = "/opt/busdata/output"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

# Dictionary to store daily files
daily_files = {}

def get_daily_file(date_str):
    """Get or create a file handle for the specified date."""
    if date_str not in daily_files:
        filename = f"{OUTPUT_DIR}/breadcrumbs_{date_str}.jsonl"
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

def main():
    """Main function to receive and process messages from Pub/Sub."""
    logger.info(f"Starting subscriber on {SUBSCRIPTION_NAME}...")
    logger.info(f"Using project: {PROJECT_ID}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    
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
        # Close all file handles
        for date_str, file_handle in daily_files.items():
            file_handle.close()
            logger.info(f"Closed file for {date_str}")
        
        # Close the subscriber client
        subscriber.close()
        logger.info("Subscriber client closed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception in main function: {e}", exc_info=True)