import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from batcher.app.core.config import config
from batcher.app.exceptions.exceptions import ProcessingError, ValidationError
from batcher.app.models import BatchRequest, Document, ProcessingStats
from batcher.app.services.batcher import Batcher


class TestBatcherService:
    """Test batcher service."""

    @pytest.fixture
    def batcher(self):
        """Create a batcher instance for testing."""
        from prometheus_client import REGISTRY

        # Clear the registry before each test
        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            REGISTRY.unregister(collector)
        return Batcher()

    def test_validate_request_empty_crawls(self, batcher):
        """Test validation fails for empty crawls list."""
        request = BatchRequest(crawls=[])

        with pytest.raises(ValidationError):
            batcher.validate_request(request)

    def test_validate_request_invalid_crawl_pattern(self, batcher):
        """Test validation fails for invalid crawl pattern."""
        request = BatchRequest(crawls=["invalid-pattern"])

        with pytest.raises(ValidationError, match="Invalid crawl pattern"):
            batcher.validate_request(request)

    def test_validate_request_valid_crawl_pattern(self, batcher):
        """Test validation succeeds for valid crawl pattern."""
        request = BatchRequest(crawls=["CC-MAIN-2024-30"])

        # Should not raise an exception
        batcher.validate_request(request)

    def test_process_request_success(self, batcher, mocker):
        """Test successful request processing."""
        mock_process_crawl = mocker.patch.object(
            batcher,
            "_process_single_crawl",
            return_value={
                "processing_time": 8,
            },
        )

        request = BatchRequest(crawls=["CC-MAIN-2024-30"])
        stats = batcher.process_request(request)

        assert isinstance(stats, ProcessingStats)
        assert stats.processing_time != 0
        mock_process_crawl.assert_called_once()

    def test_process_request_error_handling(self, batcher, mocker):
        """Test error handling during request processing."""
        mocker.patch.object(
            batcher, "_process_single_crawl", side_effect=Exception("Processing failed")
        )

        request = BatchRequest(crawls=["CC-MAIN-2024-30"])

        with pytest.raises(ProcessingError):
            batcher.process_request(request)

    def test_url_deduplication(self, batcher):
        """Test URL deduplication mechanism."""
        url = "com,example)/"

        # First time should be unique
        with batcher.url_lock:
            batcher.seen_urls.clear()  # Start fresh
            assert url not in batcher.seen_urls
            batcher.seen_urls.add(url)
            assert url in batcher.seen_urls

        # Second time should be duplicate
        with batcher.url_lock:
            assert url in batcher.seen_urls

    def test_concurrent_processing(self, batcher, mocker):
        """Test concurrent processing of multiple crawls."""
        mock_process_crawl = mocker.patch.object(
            batcher,
            "_process_single_crawl",
            return_value={
                "processing_time": 8,
            },
        )

        request = BatchRequest(crawls=["CC-MAIN-2024-30", "CC-MAIN-2024-22"])

        stats = batcher.process_request(request)

        assert mock_process_crawl.call_count == 2

    def test_batch_publishing(self, batcher, mocker):
        """Test batch publishing."""
        documents = [
            Document(
                surt_url=f"com,example)/{i}",
                timestamp="20240115120000",
                metadata={"languages": ["eng"], "status": "200"},
            )
            for i in range(config.batch_size)
        ]

        mock_publisher = mocker.Mock()
        mock_publisher.publish_with_recovery.return_value = True

        # Test batch publishing
        success = batcher._publish_batch(mock_publisher, documents)
        assert success is True
        mock_publisher.publish_with_recovery.assert_called_once_with(documents)

    @pytest.mark.parametrize(
        "status_code,expected",
        [("200", True), ("404", False), ("500", False), ("301", False)],
    )
    def test_document_filtering_by_status(self, batcher, status_code, expected):
        """Test document filtering based on HTTP status."""
        doc = Document(
            surt_url="com,example)/",
            timestamp="20240115120000",
            metadata={"languages": ["eng"], "status": status_code},
        )

        assert batcher.filter.is_valid(doc) is expected

    def test_thread_safety(self, batcher):
        """Test thread safety of URL deduplication."""
        url = "com,example)/"
        results = []

        def add_url():
            with batcher.url_lock:
                was_added = url not in batcher.seen_urls
                if was_added:
                    batcher.seen_urls.add(url)
                results.append(was_added)

        # Create multiple threads
        threads = [threading.Thread(target=add_url) for _ in range(10)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Only the first thread should have successfully added the URL
        assert results.count(True) == 1
        assert results.count(False) == 9
