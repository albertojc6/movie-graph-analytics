import requests
import time
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# --- Configuration ---
GDB_BASE_URL = "http://graphdb:7200" # Use the service name from docker-compose
REPO_ID = "moviekg"
REPO_CONFIG_FILE = "moviekg-config.ttl"
SCHEMA_FILE = "graph_model.ttl"
MAX_WAIT_SECONDS = 120

def wait_for_graphdb():
    """Waits for the GraphDB service to be accessible."""
    start_time = time.time()
    logging.info("Waiting for GraphDB to be ready...")
    while time.time() - start_time < MAX_WAIT_SECONDS:
        try:
            response = requests.get(f"{GDB_BASE_URL}/rest/repositories")
            if response.status_code == 200:
                logging.info(" GraphDB is up and running.")
                return True
        except requests.exceptions.ConnectionError:
            pass # Service is not yet available
        time.sleep(5)
    logging.error(" GraphDB did not become available within the time limit.")
    return False

def create_repository():
    """Creates the GraphDB repository using a configuration file."""
    logging.info(f"Attempting to create repository '{REPO_ID}'...")
    url = f"{GDB_BASE_URL}/rest/repositories"
    try:
        with open(REPO_CONFIG_FILE, "rb") as f:
            files = {"config": (REPO_CONFIG_FILE, f, "application/x-turtle")}
            response = requests.post(url, files=files)

        if response.status_code == 201:
            logging.info(f" Repository '{REPO_ID}' created successfully.")
        elif response.status_code == 409:
            logging.info(f" Repository '{REPO_ID}' already exists.")
        else:
            logging.error(f" Error creating repository: {response.status_code} - {response.text}")
            sys.exit(1) # Exit with an error code
    except FileNotFoundError:
        logging.error(f" Repository configuration file not found: {REPO_CONFIG_FILE}")
        sys.exit(1)


def upload_schema():
    """Uploads the schema from a TTL file to the repository."""
    logging.info(f"Uploading schema from '{SCHEMA_FILE}' to repository '{REPO_ID}'...")
    url = f"{GDB_BASE_URL}/repositories/{REPO_ID}/statements"
    headers = {"Content-Type": "application/x-turtle"}
    try:
        with open(SCHEMA_FILE, "rb") as f:
            data = f.read()
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 204:
            logging.info(" Schema uploaded successfully.")
        else:
            logging.error(f" Schema upload failed: {response.status_code} - {response.text}")
            sys.exit(1)
    except FileNotFoundError:
        logging.error(f" Schema file not found: {SCHEMA_FILE}")
        sys.exit(1)


if __name__ == "__main__":
    if wait_for_graphdb():
        create_repository()
        upload_schema()
        logging.info(" GraphDB initialization complete.")