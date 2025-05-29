#!/usr/bin/env python3
"""
Configurable data collector script for TriMet bus data.
This script can fetch different types of data based on configuration:
- breadcrumb: Vehicle breadcrumb data (default)
- stopevents: Bus stop events data

Uses concurrent.futures for parallel processing to improve performance.
"""

import os
import json
import urllib.request
import urllib.error
import datetime
import logging
import traceback
import argparse
import concurrent.futures
import time
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future as PublishFuture
from typing import List, Dict, Any, Optional, Tuple

# Import HTML parsing functions
try:
    from stop_html_parser import parse_stop_events_html, validate_stop_event_record, format_stop_event_for_output
    HTML_PARSER_AVAILABLE = True
except ImportError:
    HTML_PARSER_AVAILABLE = False
    logging.warning("HTML parser module not available - stop events parsing will be disabled")

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('data_collector.log')  # Also log to a file
    ]
)
logger = logging.getLogger('bus_data_collector')

# Base configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "dataeng-456707")
OUTPUT_DIR = "./busdata/raw_data"
MAX_WORKERS = 5  # Reduced to be gentler on the API
PUBSUB_BATCH_SIZE = 100  # Number of messages to publish in batch
REQUEST_DELAY = 0.1  # Small delay between requests to avoid overwhelming the API

# Data collection configurations
DATA_COLLECTION_CONFIGS = {
    "breadcrumb": {
        "api_url": "https://busdata.cs.pdx.edu/api/getBreadCrumbs",
        "pubsub_topic": "breadcrumb-data-topic",
        "use_vehicle_ids": True,
        "id_param": "vehicle_id",
        "data_type": "breadcrumb",
        "response_format": "json"
    },
    "stopevents": {
        "api_url": "https://busdata.cs.pdx.edu/api/getStopEvents",
        "pubsub_topic": "stop-events-topic",
        "use_vehicle_ids": True,
        "id_param": "vehicle_id",
        "data_type": "stopevents",
        "response_format": "html"  # Assuming stop events return HTML
    }
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_config(data_type: str) -> Dict[str, Any]:
    """Get configuration for the specified data type."""
    if data_type not in DATA_COLLECTION_CONFIGS:
        logger.warning(f"Unknown data type '{data_type}', using default 'breadcrumb'")
        data_type = "breadcrumb"
    
    config = DATA_COLLECTION_CONFIGS[data_type]
    logger.info(f"Using configuration for data type: {data_type}")
    logger.info(f"API URL: {config['api_url']}")
    logger.info(f"Pub/Sub Topic: {config['pubsub_topic']}")
    
    return config

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
        return ["2909", "2913", "2916"]  # Use actual vehicle IDs from your log

def read_stop_ids() -> List[str]:
    """Read stop IDs from stops.txt file (if it exists)."""
    try:
        with open("stops.txt", 'r') as f:
            stop_ids = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"Read {len(stop_ids)} stop IDs from stops.txt")
            return stop_ids
    except Exception as e:
        logger.warning(f"Could not read stops.txt, using vehicle IDs instead: {e}")
        return read_vehicle_ids()

def get_ids_for_data_type(data_type: str, config: Dict[str, Any]) -> List[str]:
    """Get appropriate IDs based on data type and configuration."""
    if data_type == "stopevents" and config["id_param"] == "stop_id":
        return read_stop_ids()
    else:
        return read_vehicle_ids()

def test_api_endpoint(config: Dict[str, Any]) -> bool:
    """Test if the API endpoint is accessible."""
    test_url = config['api_url']
    try:
        logger.info(f"Testing API endpoint: {test_url}")
        
        # Create a request with proper headers
        req = urllib.request.Request(test_url)
        req.add_header('User-Agent', 'Mozilla/5.0 (compatible; bus-data-collector/1.0)')
        
        with urllib.request.urlopen(req, timeout=10) as response:
            status_code = response.getcode()
            logger.info(f"API endpoint test successful. Status code: {status_code}")
            return True
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP Error testing API endpoint: {e.code} - {e.reason}")
        return False
    except urllib.error.URLError as e:
        logger.error(f"URL Error testing API endpoint: {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error testing API endpoint: {e}")
        return False

def fetch_data(entity_id: str, config: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Fetch data for a specific entity ID using the provided configuration."""
    url = f"{config['api_url']}?{config['id_param']}={entity_id}"
    
    try:
        logger.info(f"Fetching {config['data_type']} data for {config['id_param']} {entity_id}...")
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(REQUEST_DELAY)
        
        # Create request with proper headers
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (compatible; bus-data-collector/1.0)')
        req.add_header('Accept', 'application/json, text/html, */*')
        
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode('utf-8')
            
            # Check if we got an empty response
            if not content.strip():
                logger.warning(f"Empty response for {config['id_param']} {entity_id}")
                return []
            
            # Handle different response formats
            if config.get('response_format') == 'html':
                # Parse HTML table format for stop events
                if not HTML_PARSER_AVAILABLE:
                    logger.error("HTML parser not available - cannot process stop events data")
                    return None
                
                data = parse_stop_events_html(content, entity_id)
                
                # Validate and format the parsed data
                validated_data = []
                for record in data:
                    if validate_stop_event_record(record):
                        formatted_record = format_stop_event_for_output(record)
                        validated_data.append(formatted_record)
                    else:
                        logger.warning(f"Skipping invalid stop event record for vehicle {entity_id}")
                
                data = validated_data
            else:
                # Parse JSON format for breadcrumbs
                try:
                    data = json.loads(content)
                    # Handle case where API returns a single object instead of array
                    if isinstance(data, dict):
                        data = [data]
                    elif not isinstance(data, list):
                        logger.warning(f"Unexpected data format for {entity_id}: {type(data)}")
                        return []
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for {entity_id}: {e}")
                    logger.debug(f"Raw content (first 500 chars): {content[:500]}")
                    return None
            
            logger.info(f"Received {len(data)} records for {config['id_param']} {entity_id}")
            return data
            
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"No {config['data_type']} data found for {config['id_param']} {entity_id} (HTTP 404) - this is normal if vehicle is not active")
            return []  # Return empty list instead of None for 404s
        else:
            logger.error(f"HTTP Error fetching {config['data_type']} data for {config['id_param']} {entity_id}: {e.code} - {e.reason}")
            return None
    except urllib.error.URLError as e:
        logger.error(f"URL Error fetching {config['data_type']} data for {config['id_param']} {entity_id}: {e.reason}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching {config['data_type']} data for {config['id_param']} {entity_id}: {type(e).__name__} - {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        return None

def save_raw_data(entity_id: str, data: List[Dict[str, Any]], config: Dict[str, Any]) -> None:
    """Save raw data to a file."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"{OUTPUT_DIR}/{config['data_type']}_{entity_id}_{today}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)  # default=str handles datetime objects
        
        logger.info(f"Saved raw {config['data_type']} data for {entity_id} to {filename}")
    except Exception as e:
        logger.error(f"Error saving raw {config['data_type']} data for {entity_id}: {e}")

def publish_to_pubsub(records: List[Dict[str, Any]], config: Dict[str, Any]) -> Tuple[int, int]:
    """
    Publish individual records to Pub/Sub using futures for parallel processing.
    Returns a tuple of (published_count, error_count).
    """
    if not records:
        logger.info(f"No {config['data_type']} records to publish to Pub/Sub - no data available for this entity")
        return 0, 0
    
    # Initialize Pub/Sub publisher
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, config['pubsub_topic'])
        
        logger.info(f"Publishing {len(records)} {config['data_type']} records to Pub/Sub topic {config['pubsub_topic']}")
        
        published_count = 0
        error_count = 0
        futures = []
        
        # Create a batch of publish futures
        for record in records:
            try:
                # Add metadata to the record
                record_with_metadata = {
                    **record,
                    "data_type": config['data_type'],
                    "collection_timestamp": datetime.datetime.now().isoformat()
                }
                
                # Convert the record to a JSON string
                data = json.dumps(record_with_metadata, default=str).encode("utf-8")
                
                # Publish the message and keep track of the future
                future = publisher.publish(topic_path, data=data)
                futures.append(future)
                
                # If we've reached our batch size, wait for them to complete
                if len(futures) >= PUBSUB_BATCH_SIZE:
                    for future in futures:
                        try:
                            future.result(timeout=30)  # Wait for the future to complete with timeout
                            published_count += 1
                        except Exception as e:
                            error_count += 1
                            logger.error(f"Error publishing {config['data_type']} message: {type(e).__name__} - {str(e)}")
                    futures = []  # Clear the futures list for the next batch
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error preparing {config['data_type']} message for publishing: {type(e).__name__} - {str(e)}")
        
        # Process any remaining futures
        for future in futures:
            try:
                future.result(timeout=30)
                published_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Error publishing {config['data_type']} message: {type(e).__name__} - {str(e)}")
        
        logger.info(f"Summary: Published {published_count}/{len(records)} {config['data_type']} records to {config['pubsub_topic']}")
        if error_count > 0:
            logger.warning(f"Failed to publish {error_count} {config['data_type']} records. See logs for details.")
            
        return published_count, error_count
            
    except Exception as e:
        error_type = type(e).__name__
        logger.error(f"Fatal error initializing Pub/Sub client for {config['data_type']}: {error_type} - {str(e)}")
        logger.error(f"Check if PROJECT_ID and PUBSUB_TOPIC are correctly defined:")
        logger.error(f"PROJECT_ID: {PROJECT_ID}, PUBSUB_TOPIC: {config['pubsub_topic']}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 0, len(records) if records else 0

def process_entity(entity_id: str, config: Dict[str, Any]) -> Tuple[str, int, int]:
    """
    Process a single entity: fetch data, save raw data, and publish to Pub/Sub.
    Returns a tuple of (entity_id, published_count, error_count).
    """
    # Fetch data
    data = fetch_data(entity_id, config)
    
    if data is None:
        # Error occurred during fetching (non-404 errors)
        logger.warning(f"Skipping Pub/Sub publishing for {config['id_param']} {entity_id} due to fetch error")
        return entity_id, 0, 1
    elif not data:
        # No data returned (empty list) - this includes 404s and empty responses
        logger.info(f"No {config['data_type']} data to publish for {config['id_param']} {entity_id} - entity may be inactive or have no current data")
        return entity_id, 0, 0
    
    # Save the raw data
    save_raw_data(entity_id, data, config)
    
    # Publish individual records to Pub/Sub
    published_count, error_count = publish_to_pubsub(data, config)
    
    return entity_id, published_count, error_count

def main() -> None:
    """Main function to fetch and process data using concurrent futures."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Configurable TriMet bus data collector')
    parser.add_argument(
        '--data-type', 
        choices=['breadcrumb', 'stopevents'], 
        default='breadcrumb',
        help='Type of data to collect (default: breadcrumb)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=MAX_WORKERS,
        help=f'Maximum number of parallel workers (default: {MAX_WORKERS})'
    )
    parser.add_argument(
        '--test-api',
        action='store_true',
        help='Test API endpoint accessibility before processing'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit the number of entities to process (for testing)'
    )
    
    args = parser.parse_args()
    
    start_time = datetime.datetime.now()
    logger.info(f"Starting {args.data_type} data collection at {start_time}")
    
    # Get configuration for the specified data type
    config = get_config(args.data_type)
    
    # Test API endpoint if requested
    if args.test_api:
        if not test_api_endpoint(config):
            logger.error("API endpoint test failed. Exiting.")
            return
    
    # Get appropriate IDs for the data type
    entity_ids = get_ids_for_data_type(args.data_type, config)
    
    # Limit entities if specified
    if args.limit:
        entity_ids = entity_ids[:args.limit]
        logger.info(f"Limited processing to first {args.limit} entities")
    
    total_published = 0
    total_errors = 0
    successful_entities = 0
    
    # Process entities in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Submit all entity processing tasks
        future_to_entity = {executor.submit(process_entity, entity_id, config): entity_id for entity_id in entity_ids}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_entity):
            entity_id = future_to_entity[future]
            try:
                _, published_count, error_count = future.result()
                total_published += published_count
                total_errors += error_count
                if published_count > 0:
                    successful_entities += 1
            except Exception as e:
                logger.error(f"Error processing {config['id_param']} {entity_id}: {e}")
                total_errors += 1
    
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"{args.data_type.title()} data collection completed at {end_time}")
    logger.info(f"Total duration: {duration:.2f} seconds")
    logger.info(f"Total published: {total_published}, Total errors: {total_errors}")
    logger.info(f"Successful entities: {successful_entities}/{len(entity_ids)}")
    
    # Provide summary of why messages weren't published
    no_data_entities = len(entity_ids) - successful_entities - total_errors
    if no_data_entities > 0:
        logger.info(f"Entities with no data to publish: {no_data_entities} (likely inactive vehicles or 404 responses)")
    if total_errors > 0:
        logger.info(f"Entities with fetch errors: {total_errors} (network issues, API errors, etc.)")

if __name__ == "__main__":
    main()