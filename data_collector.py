#!/usr/bin/env python3
"""
Simple data collector script for TriMet bus breadcrumb data.
This script fetches breadcrumb data from the TriMet API and publishes
individual records to a Google Cloud Pub/Sub topic.
"""

import os
import json
import urllib.request
import datetime
from google.cloud import pubsub_v1

# Configuration - these can be imported from a config file or set directly
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "your-project-id")
PUBSUB_TOPIC = "breadcrumb-data-topic"
API_BASE_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
OUTPUT_DIR = "/opt/busdata/raw_data"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_vehicle_ids():
    """Read vehicle IDs from ids.txt file."""
    try:
        with open("ids.txt", 'r') as f:
            vehicle_ids = [line.strip() for line in f.readlines() if line.strip()]
            print(f"Read {len(vehicle_ids)} vehicle IDs from ids.txt")
            return vehicle_ids
    except Exception as e:
        print(f"Error reading vehicle IDs from file: {e}")
        # Return some default IDs as fallback
        return ["VEHICLE_ID_1", "VEHICLE_ID_2", "VEHICLE_ID_3"]

def fetch_breadcrumb_data(vehicle_id):
    """Fetch breadcrumb data for a specific vehicle ID."""
    url = f"{API_BASE_URL}?vehicle_id={vehicle_id}"
    
    try:
        print(f"Fetching data for vehicle {vehicle_id}...")
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())
            print(f"Received {len(data)} records for vehicle {vehicle_id}")
            return data
    except Exception as e:
        print(f"Error fetching data for vehicle {vehicle_id}: {e}")
        return None

def save_raw_data(vehicle_id, data):
    """Save raw data to a file."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"{OUTPUT_DIR}/vehicle_{vehicle_id}_{today}.json"
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved raw data for vehicle {vehicle_id} to {filename}")

def publish_to_pubsub(records):
    """Publish individual breadcrumb records to Pub/Sub."""
    if not records:
        return
    
    # Initialize Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    
    print(f"Publishing {len(records)} records to Pub/Sub topic {PUBSUB_TOPIC}")
    
    published_count = 0
    for record in records:
        # Convert the record to a JSON string
        data = json.dumps(record).encode("utf-8")
        
        # Publish the message
        try:
            publisher.publish(topic_path, data=data).result()
            published_count += 1
        except Exception as e:
            print(f"Error publishing record: {e}")
    
    print(f"Published {published_count} records to Pub/Sub topic {PUBSUB_TOPIC}")

def main():
    """Main function to fetch and process breadcrumb data."""
    print(f"Starting data collection at {datetime.datetime.now()}")
    
    # Read vehicle IDs from file
    vehicle_ids = read_vehicle_ids()
    
    # Process each vehicle
    for vehicle_id in vehicle_ids:
        # Fetch data
        data = fetch_breadcrumb_data(vehicle_id)
        
        if not data:
            print(f"No data received for vehicle {vehicle_id}")
            continue
        
        # Save the raw data
        save_raw_data(vehicle_id, data)
        
        # Publish individual records to Pub/Sub
        publish_to_pubsub(data)
    
    print(f"Data collection completed at {datetime.datetime.now()}")

if __name__ == "__main__":
    main()