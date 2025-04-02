import datetime
import requests
import json
import os
import time
import logging
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import flask
from flask import Flask

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TOKEN")
BUCKET_NAME = os.getenv("BUCKET_NAME", "fetch_aqicn")

# Environment-specific configurations
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))

def fetch_city_stations(city):
    """Fetch stations for a specific city with retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            search_url = f"https://api.waqi.info/search/?token={TOKEN}&keyword={city}"
            response = requests.get(search_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            stations = data.get("data", [])
            logging.info(f"Found {len(stations)} stations in {city}")
            
            # Ensure stations are dictionaries and add metadata
            valid_stations = []
            for station in stations:
                if isinstance(station, dict):
                    station["source_city"] = city  # Add city metadata
                    valid_stations.append(station)
                else:
                    logging.warning(f"Invalid station format for {city}: {station}")
            return valid_stations
        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{max_retries} failed for {city}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                logging.error(f"Failed to fetch stations for {city} after {max_retries} attempts")
                return []

def fetch_station_data(station):
    """Fetch data for a single station with error handling."""
    try:
        station_id = station.get("uid")
        if not station_id:
            logging.warning(f"Skipping station with no UID: {station}")
            return None
        
        feed_url = f"https://api.waqi.info/feed/@{station_id}/?token={TOKEN}"
        response = requests.get(feed_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get("data", {})
        
        if data:
            data["meta"] = {
                "city": station.get("source_city", "Unknown"),
                "station_id": station_id,
                "timestamp": datetime.datetime.now().isoformat()
            }
        return data
    except Exception as e:
        logging.error(f"Error processing station {station.get('uid', 'unknown')}: {e}")
        return None

def upload_to_gcs(data, bucket_name=BUCKET_NAME):
    """Upload data to Google Cloud Storage."""
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"thailand_air_quality_{timestamp}.json"
        
        # Use default credentials (set via GOOGLE_APPLICATION_CREDENTIALS or Cloud Run IAM)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            logging.info(f"Bucket {bucket_name} does not exist, creating it")
            client.create_bucket(bucket_name, location="us-central1")
        
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type='application/json'
        )
        logging.info(f"Data uploaded to gs://{bucket_name}/{blob_name}")
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {e}")
        local_file = f"/tmp/thailand_air_quality_{timestamp}.json"
        with open(local_file, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved to local file as fallback: {local_file}")
        return local_file

@app.route('/', methods=['GET', 'POST'])
def main():
    """Main function - Cloud Run entry point."""
    start_time = time.time()
    
    if not TOKEN:
        error_msg = "ERROR: TOKEN environment variable is not set!"
        logging.error(error_msg)
        return flask.jsonify({"success": False, "error": error_msg}), 500
    
    thai_cities = [
        "Bangkok", "Chiang Mai", "Phuket", "Ayutthaya", "Chonburi"
    ]
    
    # Fetch stations in parallel
    city_stations = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(thai_cities))) as executor:
        future_to_city = {executor.submit(fetch_city_stations, city): city for city in thai_cities}
        for future in future_to_city:
            city = future_to_city[future]
            try:
                stations = future.result()
                if stations:
                    city_stations[city] = stations
            except Exception as e:
                logging.error(f"Error fetching stations for {city}: {e}")
    
    # Collect unique stations
    unique_stations = {}
    for city, stations in city_stations.items():
        for station in stations:
            station_id = station.get("uid")
            if station_id and station_id not in unique_stations:
                unique_stations[station_id] = station
    
    logging.info(f"Processing {len(unique_stations)} unique stations")
    
    # Fetch station data in parallel
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_station_data, station) for station in unique_stations.values()]
        for future in futures:
            result = future.result()
            if result:
                results.append(result)
    
    # Group data by city
    data_by_city = {}
    for item in results:
        city = item.get("meta", {}).get("city", "Unknown")
        data_by_city.setdefault(city, []).append(item)
    
    # Upload to GCS
    upload_path = upload_to_gcs(data_by_city)
    
    processing_time = time.time() - start_time
    logging.info(f"Processing complete in {processing_time:.2f} seconds")
    logging.info(f"Processed {len(results)} stations across {len(data_by_city)} cities")
    
    return flask.jsonify({
        "success": True,
        "stations_processed": len(results),
        "cities_covered": list(data_by_city.keys()),
        "output_file": upload_path,
        "processing_time_seconds": processing_time
    })

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)