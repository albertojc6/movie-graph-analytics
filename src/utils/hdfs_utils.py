from hdfs import InsecureClient # type:ignore
from dotenv import load_dotenv
import os
import time
from requests.exceptions import ConnectionError, RequestException

load_dotenv('/opt/airflow/.env')

class HDFSClient:

    def __init__(self):
        self.__name_node_url = os.getenv('HDFS_URL')
        self.__user = os.getenv('HDFS_USER')

        assert self.__name_node_url is not None
        assert self.__user is not None

        self._client = InsecureClient(self.__name_node_url, user=self.__user)
        self.max_retries = 3
        self.retry_delay = 5  # seconds

    def exists(self, path):
        """Check if a path exists in HDFS"""
        return self._client.status(path, strict=False) is not None

    def list_files(self, path):
        """List all files/directories in a given path"""
        return self._client.list(path)

    def mkdirs(self, path):
        """Create directory including parent directories if needed"""
        return self._client.makedirs(path,permission=755)

    def delete(self, path, recursive=False):
        """Delete a file or directory"""
        return self._client.delete(path, recursive=recursive)

    def copy_from_local(self, local_path, hdfs_path, overwrite=True):
        """Upload file from local system to HDFS with retries"""
        self.mkdirs(hdfs_path)
        
        for attempt in range(self.max_retries):
            try:
                # Use the upload method which handles large files better
                self._client.upload(hdfs_path, local_path, overwrite=overwrite)
                return True
            except (ConnectionError, RequestException) as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise e

    def copy_to_local(self, hdfs_path, local_path, overwrite=True):
        """Download file from HDFS to local system with retries"""
        for attempt in range(self.max_retries):
            try:
                return self._client.download(hdfs_path, local_path, overwrite=overwrite)
            except (ConnectionError, RequestException) as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise e

    def read_file(self, file_path):
        """Read file contents as string"""
        with self._client.read(file_path) as reader:
            return reader.read()

    def write_file(self, file_path, content):
        """Write string content to a file"""
        with self._client.write(file_path, overwrite=True) as writer:
            writer.write(content)

    def get_size(self, path):
        """Get size of a file in bytes"""
        status = self._client.status(path)
        return status['length']




