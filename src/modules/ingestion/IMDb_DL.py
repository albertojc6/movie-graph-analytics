import concurrent.futures
from pathlib import Path
import requests
import hashlib
import time
import os

from utils.other_utils import setup_logging
from utils.hdfs_utils import HDFSClient

# Configure logging
log = setup_logging(__name__)

def calculate_file_hash(content):
    """Calculate MD5 hash of file content"""
    return hashlib.md5(content).hexdigest()

def load_IMDb(hdfs_client: HDFSClient, use_local: bool = False):
    """
    Loads some datasets from the IMDb, which consists of additional movie and authors' information
    Implements hash comparison to avoid unnecessary downloads when content hasn't changed

    Args:
        hdfs_client: object for interacting with HDFS easily.
        use_local: parameter to allow faster ingestions while testing
    """

    # Path configuration: temporal and hdfs directories
    tmp_dir = Path("/tmp/IMDb")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Create hash directory for storing file hashes
    hash_dir = tmp_dir / "hashes"
    hash_dir.mkdir(exist_ok=True)

    hdfs_dir = "/data/landing"
    
    # Files to download, already in a suitable compressed format
    files = ["name.basics.tsv.gz",  # features of movie actors and actress
             "title.basics.tsv.gz", # information about movies
             "title.crew.tsv.gz"    # enumeration of people involved in each title
            ]
    
    if not use_local:
        # Download files from web
        base_url = os.getenv("IMDb_URL")

        def download_file(file_name: str) -> None:
            """
            Download a single file using streaming to handle large content.
            Implements hash comparison to avoid unnecessary downloads.
            """
            api_url = f"{base_url}/{file_name}"
            hash_file = hash_dir / f"{file_name}.md5"
            output_path = tmp_dir / file_name
            
            try:
                log.info(f"Starting parallel download of {file_name}")
                response = requests.get(api_url, stream=True)
                response.raise_for_status()
                
                # Read the first chunk to calculate hash
                content_sample = next(response.iter_content(chunk_size=8192))
                curr_hash = calculate_file_hash(content_sample)
                
                # Check if file has changed
                if hash_file.exists():
                    with open(hash_file, 'r') as hf:
                        stored_hash = hf.read().strip()
                        if curr_hash == stored_hash:
                            return
                
                # Write the first chunk we already read
                with open(output_path, "wb") as f:
                    f.write(content_sample)
                    # Continue with the rest of the content
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Save the hash for next run comparison
                with open(hash_file, 'w') as hf:
                    hf.write(curr_hash)
                
                log.info(f"Downloaded {file_name} successfully.")
            
            except requests.exceptions.RequestException as e:
                log.error(f"Failed to download {file_name}: {str(e)}")
                raise
            except IOError as e:
                log.error(f"Failed to write {file_name}: {str(e)}")
                raise

        # Use ThreadPoolExecutor to parallelize downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(download_file, files)
    else:
        # Relative path from this file (modules/ingestion) to local data
        # Resolves to: src/modules/ingestion/local_data/IMDb
        tmp_dir = Path("/opt/airflow/data/raw/IMDb")

        # Verify local files exist
        log.info("Checking for local files...")
        missing_files = [f for f in files if not (tmp_dir / f).exists()]
        if missing_files:
            log.warning(f"Missing local files: {missing_files}. Retrieving from web...")
            load_IMDb(hdfs_client, use_local=False)
            return
        log.info("All local files present.")

    try:
        # Store in HDFS with retry logic
        log.info("Transferring files to HDFS...")
        
        # Upload each file individually with retries
        for file_name in files:
            local_path = str(tmp_dir / file_name)
            hdfs_path = f"{hdfs_dir}/IMDb/{file_name}"
            retries = 3
            
            for attempt in range(retries):
                try:
                    hdfs_client._client.upload(
                        hdfs_path,
                        local_path,
                        overwrite=True,
                        chunk_size=65536  # Reduced chunk size
                    )
                    log.info(f"Uploaded {file_name} successfully")
                    break
                except (ConnectionError, requests.exceptions.ConnectionError) as ce:
                    if attempt < retries - 1:
                        wait_time = 5 ** attempt  # Exponential backoff
                        log.warning(f"Connection error: {ce}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        log.error(f"Failed to upload {file_name} after {retries} attempts")
                        raise
                except Exception as e:
                    log.error(f"Unexpected error uploading {file_name}: {e}")
                    raise

        log.info(f"All files transferred to HDFS at {hdfs_dir}")
    except Exception as e:
        log.error(f"Error transferring data to HDFS: {e}")