import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import time
import logging
from typing import List, Optional
from pathlib import Path

from prometheus_client import Counter, Gauge

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from batcher.app.models import Document, ProcessingStats, BatchRequest
from batcher.app.utils.document_filter import DocumentFilter
from batcher.app.services.publisher import Publisher
from common.commoncrawl import CCDownloader, CSVIndexReader
from common.commoncrawl import download_cluster_idx
from batcher.app.core import config
from batcher.app.exceptions.exceptions import ValidationError, ProcessingError
from common.rabbitmq import rabbitmq_channel

logger = logging.getLogger(__name__)

class Batcher:
    """Multi-threaded batcher service for processing multiple crawls."""
    
    def __init__(self):
        self.filter = DocumentFilter()
        self.stats = ProcessingStats()
        self._init_metrics()
        self.seen_urls = set()  # Simple URL deduplication
        self.url_lock = threading.Lock()  # Thread-safe URL tracking

    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        self.total_lines = Counter("batcher_total_lines", "Total lines processed")
        self.valid_documents = Counter("batcher_valid_documents", "Valid documents")
        self.published_batches = Counter("batcher_published_batches", "Published batches")
        self.failed_batches = Counter("batcher_failed_batches", "Failed batches")
        self.file_progress = Gauge("batcher_file_progress", "File processing progress (%)")

    def validate_request(self, request: BatchRequest) -> None:
        """Validate the batch request."""
        # Validate all crawl patterns
        for crawl in request.crawls:
            if not crawl.startswith("CC-MAIN-"):
                raise ValidationError(f"Invalid crawl pattern: {crawl}")

    def process_file(self, request: BatchRequest) -> ProcessingStats:
        """Process multiple crawls using threading."""
        start_time = time.time()
        
        try:
            # Validate request
            self.validate_request(request)
            
            # Reset URL tracking
            self.seen_urls.clear()
            
            # Create shared services
            publisher = Publisher(rabbitmq_channel())
            
            logger.info(f"Processing {len(request.crawls)} crawls with {config.max_threads} threads")
            
            # Process crawls in parallel
            with ThreadPoolExecutor(max_workers=config.max_threads) as executor:
                # Submit all crawl processing tasks
                future_to_crawl = {
                    executor.submit(self._process_single_crawl, request, crawl): crawl 
                    for crawl in request.crawls
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_crawl):
                    crawl = future_to_crawl[future]
                    try:
                        crawl_stats = future.result()
                        logger.info(f"Completed crawl {crawl}: {crawl_stats}")
                    except Exception as e:
                        logger.error(f"Crawl {crawl} failed: {e}")
            
            # Calculate final stats
            self.stats.processing_time = time.time() - start_time
            
            logger.info("All crawls completed successfully")
            logger.info(f"Final stats: {self.stats}")
            logger.info(f"Unique URLs processed: {len(self.seen_urls)}")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            raise ProcessingError(f"Processing failed: {e}")

    def _process_single_crawl(self, request: BatchRequest, crawl: str) -> dict:
        """Process a single crawl in a thread."""
        logger.info(f"Starting crawl: {crawl}")
        crawl_start_time = time.time()
        publisher = Publisher(rabbitmq_channel())

        logger.info(f"{config.commoncrawl_base_url}/cc-index/collections/{crawl}/indexes crawling")

        file = download_cluster_idx(crawl)
        logger.info(file)
        
        # Create services for this thread
        downloader = CCDownloader(f"{config.commoncrawl_base_url}/cc-index/collections/{crawl}/indexes")
        index_reader = CSVIndexReader(f"./{file}")
        
        batch = []
        total_chunks = 0
        crawl_stats = {
            'crawl': crawl,
            'total_lines': 0,
            'valid_documents': 0,
            'published_batches': 0,
            'failed_batches': 0,
            'unique_urls': 0
        }
        
        try:
            for cdx_chunk in index_reader:
                total_chunks += 1
                
                # Download and process chunk
                data = downloader.download_and_unzip(
                    cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
                ).decode("utf-8")
                
                # Process each line
                for line in data.split("\n"):
                    self.total_lines.inc()
                    crawl_stats['total_lines'] += 1
                    
                    # Create document from line
                    document = Document.from_line(line)
                    if not document:
                        continue
                    
                    # Thread-safe URL deduplication
                    with self.url_lock:
                        if document.surt_url in self.seen_urls:
                            continue  # Skip duplicate URL
                        self.seen_urls.add(document.surt_url)
                        crawl_stats['unique_urls'] += 1
                    
                    # Filter document
                    if self.filter.is_valid(document):
                        self.valid_documents.inc()
                        crawl_stats['valid_documents'] += 1
                        batch.append(document)
                        
                        # Publish batch when full
                        if len(batch) >= config.batch_size:
                            success = self._publish_batch(publisher, batch)
                            if success:
                                self.published_batches.inc()
                                crawl_stats['published_batches'] += 1
                            else:
                                self.failed_batches.inc()
                                crawl_stats['failed_batches'] += 1
                            batch = []
                
                # Update progress
                progress = min(100.0, (total_chunks * 1000 / 10000) * 100)
                self.file_progress.set(progress)
            
            # Publish remaining documents
            if batch:
                success = self._publish_batch(publisher, batch)
                if success:
                    self.published_batches.inc()
                    crawl_stats['published_batches'] += 1
                else:
                    self.failed_batches.inc()
                    crawl_stats['failed_batches'] += 1
            
            crawl_stats['processing_time'] = time.time() - crawl_start_time
            logger.info(f"Completed crawl {crawl} in {crawl_stats['processing_time']:.2f}s")
            
            return crawl_stats
            
        except Exception as e:
            logger.error(f"Error processing crawl {crawl}: {e}")
            raise

    def _publish_batch(self, publisher: Publisher, batch: List[Document]) -> bool:
        """Publish a batch of documents."""
        try:
            success = publisher.publish_with_recovery(batch)
            return success
        except Exception as e:
            logger.error(f"Failed to publish batch: {e}")
            return False