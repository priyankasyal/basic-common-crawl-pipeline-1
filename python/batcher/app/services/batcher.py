import time
import logging
from typing import List, Optional
from pathlib import Path

from prometheus_client import Counter, Gauge

import sys
import os
# Add the python root directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from batcher.app.models import Document, ProcessingStats, BatchRequest
from batcher.app.utils.document_filter import DocumentFilter
from batcher.app.services.publisher import Publisher
from common.commoncrawl import CCDownloader, CSVIndexReader
from batcher.app.core import config
from batcher.app.exceptions.exceptions import ValidationError, ProcessingError
from common.rabbitmq import RabbitMQChannel

logger = logging.getLogger(__name__)


# Metrics
#total_lines = Counter("batcher_total_lines", "Total lines processed")
#valid_documents = Counter("batcher_valid_documents", "Valid documents")
#published_batches = Counter("batcher_published_batches", "Published batches")
#failed_batches = Counter("batcher_failed_batches", "Failed batches")
#file_progress = Gauge("batcher_file_progress", "File processing progress (%)")


class Batcher:
    """Simple batcher service."""
    
    def __init__(self):
        self.filter = DocumentFilter()
        self.stats = ProcessingStats()
        self._init_metrics()


    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        self.total_lines = Counter("batcher_total_lines", "Total lines processed")
        self.valid_documents = Counter("batcher_valid_documents", "Valid documents")
        self.published_batches = Counter("batcher_published_batches", "Published batches")
        self.failed_batches = Counter("batcher_failed_batches", "Failed batches")
        self.file_progress = Gauge("batcher_file_progress", "File processing progress (%)")

        
    def validate_request(self, request: BatchRequest) -> None:
        """Validate the batch request."""
        # Check file exists
        file_path = Path(request.filename)
        if not file_path.exists():
            raise ValidationError(f"File not found: {request.filename}")
        # Validate crawl pattern
        if not request.crawl.startswith("CC-MAIN-"):
            raise ValidationError(f"Invalid crawl pattern: {request.crawl}")
    
    def process_file(self, request: BatchRequest) -> ProcessingStats:
        """Process the index file."""
        start_time = time.time()
        
        try:
            # Validate request
            self.validate_request(request)
            
            # Create services
            downloader = CCDownloader(f"{config.commoncrawl_base_url}/cc-index/collections/{request.crawl}/indexes")
            index_reader = CSVIndexReader(request.filename)
            publisher = Publisher(RabbitMQChannel())
            
            # Process the file
            self._process_index(index_reader, downloader, publisher, config.batch_size)
            
            # Calculate final stats
            self.stats.processing_time = time.time() - start_time
            
            logger.info("Processing completed successfully")
            logger.info(f"Stats: {self.stats}")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            raise ProcessingError(f"Processing failed: {e}")
    
    def _process_index(self, index_reader, downloader, publisher, batch_size):
        """Process the index file."""
        batch = []
        total_chunks = 0
        
        for cdx_chunk in index_reader:
            total_chunks += 1
            
            # Download and process chunk
            data = downloader.download_and_unzip(
                cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
            ).decode("utf-8")
            
            # Process each line
            for line in data.split("\n"):
                self.total_lines.inc()
                self.stats.total_lines += 1
                
                # Create document from line
                document = Document.from_line(line)
                if not document:
                    continue
                
                # Filter document
                if self.filter.is_valid(document):
                    self.valid_documents.inc()
                    self.stats.valid_documents += 1
                    batch.append(document)
                    
                    # Publish batch when full
                    if len(batch) >= batch_size:
                        self._publish_batch(publisher, batch)
                        batch = []
            
            # Update progress
            progress = min(100.0, (total_chunks * 1000 / 10000) * 100)  # Rough estimate
            self.file_progress.set(progress)
        
        # Publish remaining documents
        if batch:
            self._publish_batch(publisher, batch)
        
        # Mark as complete
        self.file_progress.set(100.0)
    
    def _publish_batch(self, publisher: Publisher, batch: List[Document]):
        """Publish a batch of documents."""
        try:
            success = publisher.publish_with_recovery(batch)
            if success:
                self.published_batches.inc()
                self.stats.published_batches += 1
            else:
                self.failed_batches.inc()
                self.stats.failed_batches += 1
        except Exception as e:
            logger.error(f"Failed to publish batch: {e}")
            self.failed_batches.inc()
            self.stats.failed_batches += 1