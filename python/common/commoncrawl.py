from abc import ABC, abstractmethod
import os
import csv
import gzip
import logging
import time
import random
from typing import Generator, List, Optional
import requests
from requests.exceptions import RequestException, ConnectionError, Timeout


BASE_URL = "https://data.commoncrawl.org"

logger = logging.getLogger(__name__)

def download_cluster_idx(crawl: str) -> str:
        """Download idx file."""
        url = f"{BASE_URL}/cc-index/collections/{crawl}/indexes/cluster.idx"
        local_filename = f"{crawl}-cluster.idx"

        if not os.path.exists(local_filename):
            print(f"Downloading {url} -> {local_filename}")
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            print(f"Reusing cached {local_filename}")
        return local_filename


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass

class CCDownloader(Downloader):
    def __init__(self, base_url: str, max_retries: int = 3, timeout: int = 30) -> None:
        self.base_url = base_url
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        # Set reasonable timeouts and headers
        self.session.headers.update({
            'User-Agent': 'CommonCrawl-Batcher/1.0',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        """Download and decompress a chunk with retry logic and validation."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Downloading chunk: {url} bytes {start}-{start+length-1} (attempt {attempt + 1})")
                
                # Download the chunk
                buffer = self._download_chunk(url, start, length)
                
                # Validate the downloaded data
                if not self._validate_chunk(buffer, start, length):
                    raise ValueError(f"Downloaded chunk validation failed for {url}")
                
                # Decompress the data
                decompressed = self._safe_decompress(buffer, url)
                
                logger.debug(f"Successfully downloaded and decompressed chunk: {url}")
                return decompressed
                
            except (RequestException, ConnectionError, Timeout, ValueError, OSError) as e:
                last_exception = e
                logger.warning(f"Download attempt {attempt + 1} failed for {url}: {e}")
                
                if attempt < self.max_retries - 1:
                    # Exponential backoff with jitter
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries} download attempts failed for {url}")
        
        raise last_exception  

    def _download_chunk(self, url: str, start: int, length: int) -> bytes:
        """Download a specific chunk with proper headers and error handling."""
        headers = {"Range": f"bytes={start}-{start+length-1}"}
        
        try:
            response = self.session.get(
                f"{self.base_url}/{url}", 
                headers=headers, 
                timeout=self.timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Verify we got the expected content length
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) != length:
                logger.warning(f"Expected {length} bytes, got {content_length} bytes")
            
            return response.content
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 416:  # Range Not Satisfiable
                logger.error(f"Range request failed for {url}: {start}-{start+length-1}")
                raise ValueError(f"Invalid range request: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

    def _validate_chunk(self, buffer: bytes, start: int, length: int) -> bool:
        """Validate that the downloaded chunk looks reasonable."""
        if not buffer:
            logger.error("Downloaded buffer is empty")
            return False
        
        if len(buffer) == 0:
            logger.error("Downloaded buffer has zero length")
            return False
        
        # Check if it looks like gzip data (starts with gzip magic number)
        if len(buffer) >= 2:
            gzip_magic = buffer[:2]
            if gzip_magic != b'\x1f\x8b':
                logger.warning(f"Buffer doesn't start with gzip magic number: {gzip_magic.hex()}")
                # This might not be an error for some files, so we'll continue
        
        return True

    def _safe_decompress(self, buffer: bytes, url: str) -> bytes:
        """Safely decompress gzip data with detailed error reporting."""
        try:
            return gzip.decompress(buffer)
        except OSError as e:
            logger.error(f"Decompression failed for {url}: {e}")
            logger.error(f"Buffer length: {len(buffer)} bytes")
            logger.error(f"Buffer start: {buffer[:20].hex() if len(buffer) >= 20 else buffer.hex()}")
            raise ValueError(f"Failed to decompress data for {url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected decompression error for {url}: {e}")
            raise

    def __del__(self):
        """Clean up the session."""
        if hasattr(self, 'session'):
            self.session.close()


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        self.file = open(filename, "r")
        self.reader = csv.reader(self.file, delimiter="\t")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)

    def __del__(self) -> None:
        self.file.close()


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756	cdx-00000.gz	0	188224	1\n\
101,141,199,66)/robots.txt 20240714155331	cdx-00000.gz	188224	178351	2\n\
104,223,1,100)/ 20240714230020	cdx-00000.gz	366575	178055	3"
    filename.write_text(index)
    reader = CSVIndexReader(filename)
    assert list(reader) == [
        ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
        [
            "101,141,199,66)/robots.txt 20240714155331",
            "cdx-00000.gz",
            "188224",
            "178351",
            "2",
        ],
        ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
    ]
