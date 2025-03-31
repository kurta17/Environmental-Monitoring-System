import datetime
import requests
import json
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TOKEN")

def fetch_city_stations(city):
    """Fetch stations for a specific city with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            search_url = f"https://api.waqi.info/search/?token={TOKEN}&keyword={city}"
            response = requests.get(search_url, timeout=10)
            response.raise_for_status()  # Raise exception for HTTP errors
            stations = response.json().get("data", [])
            logging.info(f"Found {len(stations)} stations in {city}")
            
            # Add city metadata to each station
            for station in stations:
                station['source_city'] = city
                
            return stations
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt+1}/{max_retries} failed for {city}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retrying
            else:
                logging.error(f"Failed to fetch stations for {city} after {max_retries} attempts")
                return []

def fetch_station_data(station):
    """Fetch data for a single station with error handling"""
    try:
        # Extract station ID
        station_id = None
        if isinstance(station, dict) and "uid" in station:
            station_id = int(station["uid"])
        elif isinstance(station, dict) and "station" in station and "uid" in station.get("station", {}):
            station_id = int(station["station"]["uid"])
        else:
            logging.warning(f"Skipping station with unknown structure")
            return None
            
        # Get station data
        feed_url = f"https://api.waqi.info/feed/@{station_id}/?token={TOKEN}"
        response = requests.get(feed_url, timeout=10)
        response.raise_for_status()
        data = response.json().get("data", {})
        
        # Add metadata
        if data:
            data["meta"] = {
                "city": station.get("source_city", "Unknown"),
                "station_id": station_id,
                "timestamp": datetime.datetime.now().isoformat()
            }
        return data
    except Exception as e:
        logging.error(f"Error processing station {station.get('uid', 'unknown')}: {str(e)}")
        return None

def save_data_locally(data):
    """Save data to local files"""
    # Create a data directory if it doesn't exist
    output_dir = "air_quality_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create unique filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{output_dir}/thailand_air_quality_{timestamp}.json"
    
    # Save main data file
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    logging.info(f"Data saved to {filename}")
    
    # Save individual city files
    for city, city_data in data.items():
        city_filename = f"{output_dir}/{city}_air_quality_{timestamp}.json"
        with open(city_filename, "w") as f:
            json.dump(city_data, f, indent=2)
        logging.info(f"{city} data saved to {city_filename}")
    
    # Save summary file
    summary_filename = f"{output_dir}/summary_{timestamp}.json"
    with open(summary_filename, "w") as f:
        json.dump({
            "timestamp": datetime.datetime.now().isoformat(),
            "cities": list(data.keys()),
            "station_counts": {city: len(stations) for city, stations in data.items()},
            "total_stations": sum(len(stations) for stations in data.values())
        }, f, indent=2)
    
    return filename

def main():
    """Main function for local testing"""
    start_time = time.time()
    
    # List of major cities in Thailand
    thai_cities = [
        "Bangkok", "Chiang Mai", "Phuket", "Ayutthaya", "Chonburi"
    ]
    
    if not TOKEN:
        logging.error("API TOKEN is missing! Please set it in your .env file.")
        return
    
    # Fetch stations for each city in parallel
    city_stations = {}
    with ThreadPoolExecutor(max_workers=min(5, len(thai_cities))) as executor:
        future_to_city = {executor.submit(fetch_city_stations, city): city for city in thai_cities}
        for future in future_to_city:
            city = future_to_city[future]
            try:
                stations = future.result()
                if stations:
                    city_stations[city] = stations
            except Exception as e:
                logging.error(f"Error fetching stations for {city}: {str(e)}")
    
    # Process all unique stations
    all_stations = []
    for stations in city_stations.values():
        all_stations.extend(stations)
    
    # Remove duplicates
    unique_stations = {}
    for station in all_stations:
        station_id = station.get("uid") or station.get("station", {}).get("uid")
        if station_id and station_id not in unique_stations:
            unique_stations[station_id] = station
    
    logging.info(f"Processing {len(unique_stations)} unique stations")
    
    # Fetch data for all stations with parallelism
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_station_data, station) 
                  for station in unique_stations.values()]
        for future in futures:
            try:
                data = future.result()
                if data:
                    results.append(data)
            except Exception as e:
                logging.error(f"Error in station data processing: {str(e)}")
    
    # Group data by city
    data_by_city = {}
    for item in results:
        city = item.get("meta", {}).get("city", "Unknown")
        if city not in data_by_city:
            data_by_city[city] = []
        data_by_city[city].append(item)
    
    # Save the results locally
    output_file = save_data_locally(data_by_city)
    
    processing_time = time.time() - start_time
    logging.info(f"Processing complete in {processing_time:.2f} seconds")
    logging.info(f"Processed {len(results)} stations across {len(data_by_city)} cities")
    logging.info(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()