from pathlib import Path
import requests
import hashlib
import os

from utils.other_utils import setup_logging
from utils.hdfs_utils import HDFSClient

# Configure logging
log = setup_logging(__name__)

def calculate_file_hash(content):
    """Calculate MD5 hash of file content"""
    return hashlib.md5(content).hexdigest()

def load_MovieTweetings(hdfs_client: HDFSClient):
    """
    Loads the MovieTweetings Dataset, which consists of ratings extracted from tweets: users.dat, items.dat & ratings.dat
    Allow for incremental data ingestion, with datasets' hash comparison

    Args:
        hdfs_client: object for interacting with HDFS easily.
        max_rows: number of ratings to store in HDFS
    """

    # Path coniguration: temporal and hdfs directories
    tmp_dir = Path("/tmp/MovieTweetings")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    hdfs_dir = "/data/landing/MovieTweetings"

    # Use a directory for hash storage
    hash_dir = Path("/tmp/hashes")
    hash_dir.mkdir(exist_ok=True)
    
    # URL for retrieving 200k ratings
    base_url = os.getenv("MovieTweetings_URL")
    files = ["movies.dat", "ratings.dat", "users.dat"]
    
    for f in files:
        # First check if file exists in HDFS
        hash_file = hash_dir / f"{f}.md5"
        
        # Download current version from source
        api_url = f"{base_url}/{f}"
        response = requests.get(api_url)

        if response.status_code != 200:
            log.warning(f"Failed to download {f}. Status code: {response.status_code}")
            continue
        
        # Calculate hash of current content
        curr_content = response.content
        curr_hash = calculate_file_hash(curr_content)
        
        # Check if file has changed
        if hash_file.exists():
            with open(hash_file, 'r') as hf:
                stored_hash = hf.read().strip()
                if curr_hash == stored_hash:
                    log.info(f"File {f} hasn't changed since last run. Skipping processing.")
                    continue
            
        # File has changed, process it
        log.info(f"Changes detected in {f}. Processing...")
        output_file = tmp_dir / f
        
        with open(output_file, "wb") as local_f:
            local_f.write(curr_content)
            log.info(f"File {f} downloaded!")
        
        try:
            # Store raw .dat file in HDFS
            hdfs_client.copy_from_local(str(output_file), hdfs_dir)
            log.info(f"Transferred {f} to HDFS")
            
            # Save the hash for next run comparison
            with open(hash_file, 'w') as hf:
                hf.write(curr_hash)
                log.info(f"Updated hash for {f} for future change detection")
                
        except Exception as e:
            log.error(f"Error processing {f}: {e}")
            raise