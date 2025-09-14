import pytest
from unittest.mock import Mock, patch

import sys
import os

# Add the python root directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from batcher.app.models import Document, BatchRequest
from batcher.app.utils.document_filter import DocumentFilter
from batcher.app.services.batcher import Batcher
from batcher.app.exceptions.exceptions import ValidationError


class TestDocumentFilter:
    """Test document filtering."""
    
    def test_valid_document(self):
        """Test valid document passes filter."""
        doc = Document(
            surt_url="com,example)/",
            timestamp="20240115120000",
            metadata={"languages": ["eng"], "status": "200"}
        )
        
        assert DocumentFilter.is_valid(doc) is True
    
    def test_invalid_language(self):
        """Test document with wrong language is rejected."""
        doc = Document(
            surt_url="com,example)/",
            timestamp="20240115120000",
            metadata={"languages": ["fr"], "status": "200"}
        )
        
        assert DocumentFilter.is_valid(doc) is False
    
    def test_invalid_status(self):
        """Test document with wrong status is rejected."""
        doc = Document(
            surt_url="com,example)/",
            timestamp="20240115120000",
            metadata={"languages": ["eng"], "status": "404"}
        )
        
        assert DocumentFilter.is_valid(doc) is False


class TestBatcherService:
    """Test batcher service."""
    
    def test_validate_request_file_not_found(self):
        """Test validation fails for missing file."""
        service = Batcher()
        request = BatchRequest(filename="nonexistent.csv", crawl="CC-MAIN-2024-30")
        
        with pytest.raises(ValidationError):
            service.validate_request(request)


class TestDocument:
    """Test document model."""
    
    def test_from_line_valid(self):
        """Test creating document from valid line."""
        line = "com,example)/ 20240115120000 {\"status\":\"200\",\"languages\":[\"eng\"]}"
        doc = Document.from_line(line)
        
        assert doc is not None
        assert doc.surt_url == "com,example)/"
        assert doc.timestamp == "20240115120000"
        assert doc.metadata["status"] == "200"
    
    def test_from_line_invalid(self):
        """Test creating document from invalid line."""
        line = "invalid line"
        doc = Document.from_line(line)
        
        assert doc is None
    
    def test_to_dict(self):
        """Test converting document to dictionary."""
        doc = Document(
            surt_url="com,example)/",
            timestamp="20240115120000",
            metadata={"status": "200"}
        )
        
        result = doc.to_dict()
        assert result["surt_url"] == "com,example)/"
        assert result["timestamp"] == "20240115120000"
        assert result["metadata"]["status"] == "200"