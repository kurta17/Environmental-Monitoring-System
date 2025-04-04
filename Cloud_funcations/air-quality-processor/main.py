import functions_framework
import base64
import json
import os
from google.cloud import storage
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
BUCKET_NAME = os.getenv("BUCKET_NAME", "fetch_aqicn")
PROJECT_ID = os.getenv("PROJECT_ID", "propane-net-455409-s5")
DATASET_ID = "air_quality"
TABLE_ID = "raw_data"

# Initialize clients
storage_client = storage.Client()
bq_client = bigquery.Client()

def process_json_data(file_path):
    """Process JSON data from GCS and prepare for BigQuery."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_path)
    try:
        json_data = json.loads(blob.download_as_string())
    except Exception as e:
        logging.error(f"Failed to parse JSON file {file_path}: {e}")
        return []

    rows_to_insert = []
    for city, stations in json_data.items():
        for station in stations:
            try:
                # Validate aqi
                aqi = station.get("aqi")
                if aqi == "-" or aqi is None:
                    logging.warning(f"Skipping station {station.get('idx', 'unknown')} in {city}: Invalid aqi value: {aqi}")
                    continue
                try:
                    aqi = int(aqi)  # Ensure aqi is an integer
                except (ValueError, TypeError):
                    logging.warning(f"Skipping station {station.get('idx', 'unknown')} in {city}: Cannot convert aqi to integer: {aqi}")
                    continue

                # Validate timestamp
                timestamp = station.get("time", {}).get("iso")
                if not timestamp:
                    logging.warning(f"Skipping station {station.get('idx', 'unknown')} in {city}: Empty or missing timestamp")
                    continue

                # Build row for BigQuery
                row = {
                    "station_id": station.get("idx"),
                    "city": station.get("meta", {}).get("city", "Unknown"),
                    "timestamp": timestamp,
                    "aqi": aqi,
                    "pm25": station.get("iaqi", {}).get("pm25", {}).get("v", None),
                    "pm10": station.get("iaqi", {}).get("pm10", {}).get("v", None),
                    "temperature": station.get("iaqi", {}).get("t", {}).get("v", None),
                    "humidity": station.get("iaqi", {}).get("h", {}).get("v", None),
                    "latitude": station.get("city", {}).get("geo", [None, None])[0],
                    "longitude": station.get("city", {}).get("geo", [None, None])[1]
                }
                rows_to_insert.append(row)
            except Exception as e:
                logging.error(f"Error processing station {station.get('idx', 'unknown')} in {city}: {e}")
                continue
    return rows_to_insert

def append_to_bigquery(rows):
    """Append processed rows to BigQuery without retries."""
    if not rows:
        logging.info("No valid rows to insert into BigQuery")
        return True  # Success if no rows to insert

    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        logging.error(f"Errors inserting rows: {errors}")
        raise Exception(f"Failed to insert rows into BigQuery: {errors}")
    else:
        logging.info(f"Successfully appended {len(rows)} rows to BigQuery")
        return True

@functions_framework.cloud_event
def process_gcs_file(cloud_event):
    """Handle Pub/Sub message triggered by GCS file upload."""
    # Extract Pub/Sub message data
    try:
        data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        attributes = cloud_event.data["message"].get("attributes", {})
    except Exception as e:
        logging.error(f"Failed to decode Pub/Sub message: {e}")
        return {"success": False}, 400  # Bad request, retry

    # Get GCS file path
    file_path = attributes.get("objectId")
    if not file_path:
        logging.error("No file path in Pub/Sub message")
        return {"success": False}, 400  # Bad request, retry

    logging.info(f"Processing file: {file_path}")

    # Process and append data
    rows = process_json_data(file_path)
    try:
        append_to_bigquery(rows)
        return {"success": True}, 200  # Success, acknowledge message
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        return {"success": False}, 500  # Internal error, trigger Pub/Sub retry and DLQ after 5 attempts
