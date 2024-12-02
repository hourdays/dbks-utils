import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Replace with your Databricks instance URL and token
DATABRICKS_INSTANCE = "https://your-instance.cloud.databricks.com"
TOKEN = "your-token"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

# Output file
OUTPUT_FILE = "batched_databricks_notebooks.json"

# Global lock for thread-safe operations
lock = threading.Lock()

# Function to list objects in a given path
def list_workspace_objects(path):
    try:
        response = requests.get(
            f"{DATABRICKS_INSTANCE}/api/2.0/workspace/list",
            headers=HEADERS,
            json={"path": path},
            timeout=60
        )
        response.raise_for_status()
        data = response.json()
        return data.get("objects", [])
    except Exception as e:
        logging.error(f"Error listing objects at {path}: {str(e)}")
        return []

# Recursive function to process notebooks in a directory
def process_directory(path, processed_paths, output_data):
    items = list_workspace_objects(path)

    for item in items:
        if item["object_type"] == "NOTEBOOK" and item["path"] not in processed_paths:
            notebook_data = {
                "path": item["path"],
                "name": item["path"].split("/")[-1],
                "language": item.get("language", "UNKNOWN")
            }
            with lock:
                output_data.append(notebook_data)
                write_to_file(notebook_data)
                logging.info(f"Discovered notebook: {notebook_data['path']}")
        elif item["object_type"] == "DIRECTORY":
            directories_to_process.append(item["path"])

# Function to write a single record to the output file incrementally
def write_to_file(record):
    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

# Main function to scan directories in parallel
def list_notebooks_in_batches():
    directories_to_process.extend(["/Users", "/Shared", "/Repos"])

    # Load already processed paths
    processed_paths = {notebook["path"] for notebook in load_existing_data()}

    # Process directories concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        while directories_to_process:
            futures = [
                executor.submit(process_directory, path, processed_paths, [])
                for path in directories_to_process
            ]
            directories_to_process.clear()  # Clear the list after submission

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing directory: {str(e)}")

# Load existing progress from the output file
def load_existing_data():
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r") as f:
            lines = f.readlines()
        existing_data = [json.loads(line.strip()) for line in lines]
        logging.info(f"Resuming from {len(existing_data)} records in {OUTPUT_FILE}")
        return existing_data
    return []

# Global list to track directories to process
directories_to_process = []

# Run the script
if __name__ == "__main__":
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w") as f:
            pass  # Create an empty file if it doesn't exist

    list_notebooks_in_batches()
    logging.info(f"Script completed. Output written to {OUTPUT_FILE}")
