import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        logging.info(f"Successfully listed objects in: {path}")
        return data.get("objects", [])
    except Exception as e:
        logging.error(f"Error listing objects at {path}: {str(e)}")
        return []

# Recursive function to get all notebooks
def get_all_notebooks(path, output_data):
    items = list_workspace_objects(path)

    for item in items:
        if item["object_type"] == "NOTEBOOK":
            notebook_data = {
                "path": item["path"],
                "name": item["path"].split("/")[-1],
                "language": item.get("language", "UNKNOWN")
            }
            output_data.append(notebook_data)
            write_to_file(notebook_data)  # Write notebook to file immediately
            logging.info(f"Found notebook: {notebook_data['path']}")
        elif item["object_type"] == "DIRECTORY":
            logging.info(f"Recursing into directory: {item['path']}")
            get_all_notebooks(item["path"], output_data)

# Function to write a single record to the output file incrementally
def write_to_file(record):
    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

# Load existing progress from the output file
def load_existing_data():
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r") as f:
            lines = f.readlines()
        existing_data = [json.loads(line.strip()) for line in lines]
        logging.info(f"Resuming from {len(existing_data)} records in {OUTPUT_FILE}")
        return existing_data
    return []

# Main function to scan top-level directories in parallel
def list_notebooks_in_batches():
    top_level_paths = ["/Users", "/Shared", "/Repos"]
    all_notebooks = load_existing_data()  # Load existing data to avoid duplicates

    existing_paths = {notebook["path"] for notebook in all_notebooks}  # Paths already processed

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_path = {
            executor.submit(get_all_notebooks, path, all_notebooks): path for path in top_level_paths
            if path not in existing_paths
        }
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            try:
                future.result()
                logging.info(f"Completed scanning: {path}")
            except Exception as e:
                logging.error(f"Error processing {path}: {str(e)}")

# Run the script
if __name__ == "__main__":
    # Ensure the output file exists
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w") as f:
            pass  # Create an empty file if it doesn't exist

    list_notebooks_in_batches()
    logging.info(f"Script completed. Output written to {OUTPUT_FILE}")
