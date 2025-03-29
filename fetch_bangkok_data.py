import requests
import json
import os
from dotenv import load_dotenv


load_dotenv()
TOKEN = os.getenv("TOKEN")


def fetch_bangkok_stations():
    """Fetch list of air quality monitoring stations in Bangkok."""
    search_url = f"https://api.waqi.info/search/?token={TOKEN}&keyword=Bangkok"
    response = requests.get(search_url)
    return response.json().get("data", [])

def fetch_station_data(station_id):
    """Fetch air quality and temperature data for a specific station."""
    feed_url = f"https://api.waqi.info/feed/@{station_id}/?token={TOKEN}"
    response = requests.get(feed_url)
    return response.json().get("data", {})

def main():
    # Fetch stations
    stations = fetch_bangkok_stations()
    if not stations:
        print("No stations found for Bangkok.")
        return
    
    
    # Fetch data for each station
    data = []
    for station in stations:
        try:
            # The API likely returns station data in a different format
            if isinstance(station, dict) and "uid" in station:
                station_id = int(station["uid"])
            elif isinstance(station, dict) and "station" in station and "uid" in station.get("station", {}):
                station_id = int(station["station"]["uid"])
            else:
                print(f"Skipping station with unknown structure: {station}")
                continue
                
            station_data = fetch_station_data(station_id)
            if station_data:
                data.append(station_data)
        except (KeyError, ValueError, TypeError) as e:
            print(f"Error processing station: {e}")
            print(f"Station data: {station}")
    
    # Dump the raw JSON data to a file
    with open("bangkok_air_quality.json", "w") as f:
        json.dump(data, f, indent=4)
    print("Data saved to bangkok_air_quality.json")

if __name__ == "__main__":
    main()