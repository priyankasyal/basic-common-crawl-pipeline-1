import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from prometheus_client import Counter, Gauge

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from batcher.app.core import config
from batcher.app.exceptions.exceptions import ProcessingError, ValidationError
from batcher.app.models import BatchRequest, Document, ProcessingStats
from batcher.app.services.publisher import Publisher
from batcher.app.utils.document_filter import DocumentFilter
from common.commoncrawl import CCDownloader, CSVIndexReader, download_cluster_idx
from common.rabbitmq import rabbitmq_channel
from common.utils import count_file_lines

logger = logging.getLogger(__name__)


class Batcher:
    """Multi-threaded batcher service for processing multiple crawls."""

    def __init__(self):
        self.filter = DocumentFilter()
        self.stats = ProcessingStats()
        self._init_metrics()
        self.seen_urls = set()  # URL deduplication
        self.url_lock = threading.Lock()  # Thread-safe URL tracking

    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        # Processing metrics
        self.lines_total = Counter(
            "batcher_lines_total",
            "Total number of lines processed from CDX files",
            ["crawl"],
        )
        self.valid_documents_total = Counter(
            "batcher_valid_documents_total",
            "Number of valid documents that passed filtering",
            ["crawl"],
        )

        # Batch metrics
        self.batches_published_total = Counter(
            "batcher_batches_published_total",
            "Number of batches successfully published to queue",
            ["crawl"],
        )
        self.batches_failed_total = Counter(
            "batcher_batches_failed_total",
            "Number of batches that failed to publish",
            ["crawl", "error"],
        )

        # URL metrics
        self.urls_processed_total = Counter(
            "batcher_urls_processed_total",
            "Total number of unique URLs processed",
            ["crawl"],
        )
        self.urls_filtered_total = Counter(
            "batcher_urls_filtered_total",
            "Number of URLs filtered out by various criteria",
            ["crawl", "reason"],
        )
        self.urls_duplicate_total = Counter(
            "batcher_urls_duplicate_total",
            "Number of duplicate URLs encountered",
            ["crawl"],
        )

        # CDX chunk metrics
        self.cdx_chunks_processed_total = Counter(
            "batcher_cdx_chunks_processed_total",
            "Number of CDX chunks processed",
            ["crawl"],
        )
        self.cdx_chunks_failed_total = Counter(
            "batcher_cdx_chunks_failed_total",
            "Number of CDX chunks that failed processing",
            ["crawl", "error"],
        )

        # Performance metrics
        self.processing_duration_seconds = Gauge(
            "batcher_processing_duration_seconds",
            "Time taken to process each crawl",
            ["crawl"],
        )
        self.processing_rate = Gauge(
            "batcher_processing_rate_per_second",
            "Number of documents processed per second",
            ["crawl"],
        )
        self.errors_counter = Counter(
            "batcher_errors_total", "Processing errors", ["error_type"]
        )

        # Progress monitoring metrics
        self.file_progress = Gauge(
            "batcher_file_progress", "File processing progress per crawl (%)", ["crawl"]
        )
        self.total_index_lines_gauge = Gauge(
            "batcher_total_index_lines",
            "Total number of lines in the index file",
            ["crawl"],
        )
        self.processed_index_lines_counter = Counter(
            "batcher_processed_index_lines_total",
            "Number of index lines processed",
            ["crawl"],
        )

    def validate_request(self, request: BatchRequest) -> None:
        """Validate the batch request."""
        if not request.crawls:
            raise ValidationError("Crawls list cannot be empty")

        # Validate all crawl patterns
        for crawl in request.crawls:
            if not crawl.startswith("CC-MAIN-"):
                raise ValidationError(f"Invalid crawl pattern: {crawl}")

    def process_request(self, request: BatchRequest) -> ProcessingStats:
        start_time = time.time()

        try:
            # Validate request
            self.validate_request(request)

            # Reset URL tracking
            self.seen_urls.clear()
            logger.info(f"Processing {len(request.crawls)} crawls")

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
                        # Update stats from crawl results
                        self.stats.processing_time += crawl_stats.get(
                            "processing_time", 0.0
                        )
                        self.processing_duration_seconds.labels(crawl=crawl).set(
                            crawl_stats.get("processing_time", 0.0)
                        )
                        logger.info(f"Completed crawl {crawl}: {crawl_stats}")
                    except Exception as e:
                        logger.error(f"Crawl {crawl} failed: {e}")
                        raise ProcessingError(f"Processing failed: {e}")

            # Calculate final stats and update metrics
            processing_time = time.time() - start_time
            self.stats.processing_time = processing_time

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

        file = download_cluster_idx(crawl)

        # Create services for this thread
        downloader = CCDownloader(
            f"{config.commoncrawl_base_url}/cc-index/collections/{crawl}/indexes"
        )
        index_reader = CSVIndexReader(f"./{file}")

        batch = []
        total_chunks = 0
        crawl_stats = {
            "crawl": crawl,
            "procsseing_time": 0,
        }

        try:
            # Count total lines in the index file for accurate progress tracking
            index_file_path = f"./{file}"
            total_lines = count_file_lines(index_file_path)
            self.total_index_lines_gauge.labels(crawl=crawl).set(total_lines)

            for cdx_chunk in index_reader:
                total_chunks += 1
                self.cdx_chunks_processed_total.labels(crawl=crawl).inc()

                # Update progress based on actual line count
                progress = (total_chunks / total_lines) * 100
                self.file_progress.labels(crawl=crawl).set(progress)
                self.processed_index_lines_counter.labels(crawl=crawl).inc()

                try:
                    # Download and process chunk
                    data = downloader.download_and_unzip(
                        cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
                    ).decode("utf-8")
                except Exception as e:
                    self.cdx_chunks_failed_total.labels(
                        crawl=crawl, error=type(e).__name__
                    ).inc()
                    logger.error(f"Failed to process CDX chunk: {e}")
                    continue

                # Process each line
                for line in data.split("\n"):
                    self.lines_total.labels(crawl=crawl).inc()

                    # Create document from line
                    document = Document.from_line(line)
                    if not document:
                        self.urls_filtered_total.labels(
                            crawl=crawl, reason="invalid_format"
                        ).inc()
                        continue

                    # Thread-safe URL deduplication
                    with self.url_lock:
                        if document.surt_url in self.seen_urls:
                            self.urls_duplicate_total.labels(crawl=crawl).inc()
                            continue  # Skip duplicate URL
                        self.seen_urls.add(document.surt_url)
                        self.urls_processed_total.labels(crawl=crawl).inc()

                    # Filter document
                    if self.filter.is_valid(document):
                        self.valid_documents_total.labels(crawl=crawl).inc()
                        batch.append(document)

                        # Publish batch when full
                        if len(batch) >= config.batch_size:
                            try:
                                success = self._publish_batch(publisher, batch)
                                if success:
                                    self.batches_published_total.labels(
                                        crawl=crawl
                                    ).inc()
                                else:
                                    self.batches_failed_total.labels(
                                        crawl=crawl, error="publish_failed"
                                    ).inc()
                            except Exception as e:
                                self.batches_failed_total.labels(
                                    crawl=crawl, error=type(e).__name__
                                ).inc()
                            batch = []
                    else:
                        self.urls_filtered_total.labels(
                            crawl=crawl, reason="filter_failed"
                        ).inc()

                # Update progress metrics
                self.processed_index_lines_counter.labels(crawl=crawl).inc()
                total_lines = (
                    index_reader.total_lines
                    if hasattr(index_reader, "total_lines")
                    else 10000
                )
                progress = min(100.0, (total_chunks / (total_lines / 1000)) * 100)
                self.file_progress.labels(crawl=crawl).set(progress)

                if total_chunks == 1:  # Set total lines gauge at the start
                    self.total_index_lines_gauge.labels(crawl=crawl).set(total_lines)

            # Publish remaining documents
            if batch:
                try:
                    success = self._publish_batch(publisher, batch)
                    if success:
                        self.batches_published_total.labels(crawl=crawl).inc()
                    else:
                        self.batches_failed_total.labels(
                            crawl=crawl, error="publish_failed"
                        ).inc()
                except Exception as e:
                    self.batches_failed_total.labels(
                        crawl=crawl, error=type(e).__name__
                    ).inc()

            crawl_stats["processing_time"] = time.time() - crawl_start_time
            logger.info(
                f"Completed crawl {crawl} in {crawl_stats['processing_time']:.2f}s"
            )

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
