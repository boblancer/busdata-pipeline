#!/usr/bin/env python3
"""
Simple data collector script for TriMet bus breadcrumb data.
This script fetches breadcrumb data from the TriMet API and publishes
individual records to a Google Cloud Pub/Sub topic.
Uses concurrent.futures for parallel processing to improve performance.
"""

import os
import json
import urllib.request
import datetime
import logging
import traceback
import concurrent.futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future as PublishFuture
from typing import List, Dict, Any, Optional, Tuple

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('data_collector.log')  # Also log to a file
    ]
)
logger = logging.getLogger('breadcrumb_collector')

# Configuration - these can be imported from a config file or set directly
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "dataeng-456707")
PUBSUB_TOPIC = "breadcrumb-data-topic"
API_BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
OUTPUT_DIR = "./busdata/raw_data"
MAX_WORKERS = 10  # Maximum number of parallel workers for vehicle processing
PUBSUB_BATCH_SIZE = 100  # Number of messages to publish in batch

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_vehicle_ids() -> List[str]:
    """Read vehicle IDs from ids.txt file."""
    try:
        with open("ids.txt", 'r') as f:
            vehicle_ids = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Read {len(vehicle_ids)} vehicle IDs from ids.txt")
            return vehicle_ids
    except Exception as e:
        logger.error(f"Error reading vehicle IDs from file: {e}")
        # Return some default IDs as fallback
        return ["VEHICLE_ID_1", "VEHICLE_ID_2", "VEHICLE_ID_3"]

def fetch_breadcrumb_data(vehicle_id: str) -> Optional[List[Dict[str, Any]]]:
    """Fetch breadcrumb data for a specific vehicle ID."""
    url = f"{API_BASE_URL}?vehicle_id={vehicle_id}"
    
    try:
        logger.info(f"Fetching data for vehicle {vehicle_id}...")
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())
            logger.info(f"Received {len(data)} records for vehicle {vehicle_id}")
            return data
    except Exception as e:
        logger.error(f"Error fetching data for vehicle {vehicle_id}: {e}")
        return None

def save_raw_data(vehicle_id: str, data: List[Dict[str, Any]]) -> None:
    """Save raw data to a file."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"{OUTPUT_DIR}/vehicle_{vehicle_id}_{today}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved raw data for vehicle {vehicle_id} to {filename}")
    except Exception as e:
        logger.error(f"Error saving raw data for vehicle {vehicle_id}: {e}")

def publish_to_pubsub(records: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Publish individual breadcrumb records to Pub/Sub using futures for parallel processing.
    Returns a tuple of (published_count, error_count).
    """
    if not records:
        logger.warning("No records to publish to Pub/Sub")
        return 0, 0
    
    # Initialize Pub/Sub publisher
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
        
        logger.info(f"Publishing {len(records)} records to Pub/Sub topic {PUBSUB_TOPIC}")
        
        published_count = 0
        error_count = 0
        futures = []
        
        # Create a batch of publish futures
        for record in records:
            # Convert the record to a JSON string
            data = json.dumps(record).encode("utf-8")
            
            # Publish the message and keep track of the future
            future = publisher.publish(topic_path, data=data)
            futures.append(future)
            
            # If we've reached our batch size, wait for them to complete
            if len(futures) >= PUBSUB_BATCH_SIZE:
                for future in futures:
                    try:
                        future.result()  # Wait for the future to complete
                        published_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error publishing message: {type(e).__name__} - {str(e)}")
                futures = []  # Clear the futures list for the next batch
        
        # Process any remaining futures
        for future in futures:
            try:
                future.result()
                published_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Error publishing message: {type(e).__name__} - {str(e)}")
        
        logger.info(f"Summary: Published {published_count}/{len(records)} records to {PUBSUB_TOPIC}")
        if error_count > 0:
            logger.warning(f"Failed to publish {error_count} records. See logs for details.")
            
        return published_count, error_count
            
    except Exception as e:
        error_type = type(e).__name__
        logger.error(f"Fatal error initializing Pub/Sub client: {error_type} - {str(e)}")
        logger.error(f"Check if PROJECT_ID and PUBSUB_TOPIC are correctly defined:")
        logger.error(f"PROJECT_ID: {PROJECT_ID}, PUBSUB_TOPIC: {PUBSUB_TOPIC}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 0, len(records) if records else 0

def process_vehicle(vehicle_id: str) -> Tuple[str, int, int]:
    """
    Process a single vehicle: fetch data, save raw data, and publish to Pub/Sub.
    Returns a tuple of (vehicle_id, published_count, error_count).
    """
    # Fetch data
    data = fetch_breadcrumb_data(vehicle_id)
    
    if not data:
        logger.warning(f"No data received for vehicle {vehicle_id}")
        return vehicle_id, 0, 0
    
    # Save the raw data
    save_raw_data(vehicle_id, data)
    
    # Publish individual records to Pub/Sub
    published_count, error_count = publish_to_pubsub(data)
    
    return vehicle_id, published_count, error_count

def main() -> None:
    """Main function to fetch and process breadcrumb data using concurrent futures."""
    start_time = datetime.datetime.now()
    logger.info(f"Starting data collection at {start_time}")
    
    # Read vehicle IDs from file
    vehicle_ids = read_vehicle_ids()
    
    total_published = 0
    total_errors = 0
    
    # Process vehicles in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all vehicle processing tasks
        future_to_vehicle = {executor.submit(process_vehicle, vehicle_id): vehicle_id for vehicle_id in vehicle_ids}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_vehicle):
            vehicle_id = future_to_vehicle[future]
            try:
                _, published_count, error_count = future.result()
                total_published += published_count
                total_errors += error_count
            except Exception as e:
                logger.error(f"Error processing vehicle {vehicle_id}: {e}")
    
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Data collection completed at {end_time}")
    logger.info(f"Total duration: {duration:.2f} seconds")
    logger.info(f"Total published: {total_published}, Total errors: {total_errors}")

if __name__ == "__main__":
    main()