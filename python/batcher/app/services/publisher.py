"""Simple message publisher with retry logic."""

import json
import logging
from typing import List
from prometheus_client import Counter

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from batcher.app.models import Document
from common.rabbitmq import RabbitMQChannel, QUEUE_NAME
from batcher.app.utils.retry_utils import retry
from batcher.app.exceptions import PublishingError
from batcher.app.core.config import config

logger = logging.getLogger(__name__)

# Metrics


class Publisher:
    """Simple message publisher."""
    
    def __init__(self, channel: RabbitMQChannel):
        self.channel = channel
        self._init_metrics()


    def _init_metrics(self):
        """Initialize Prometheus metrics."""
        self.publish_success = Counter("batcher_publish_success", "Successful publications")
        self.publish_failure = Counter("batcher_publish_failure", "Failed publications")
        self.publish_retry = Counter("batcher_publish_retry", "Retry attempts")
 
    @retry(max_attempts=config.max_retries, delay=config.retry_delay, exceptions=(Exception,))
    def publish_batch(self, documents: List[Document]) -> bool:
        """Publish a batch of documents."""
        try:
            # Convert documents to JSON
            batch_data = [doc.to_dict() for doc in documents]
            message = json.dumps(batch_data)
            
            # Publish message
            self.channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=message
            )
            
            self.publish_success.inc()
            logger.info(f"Published batch of {len(documents)} documents")
            return True
            
        except Exception as e:
            self.publish_failure.inc()
            logger.error(f"Failed to publish batch: {e}")
            raise PublishingError(f"Publishing failed: {e}")
    
    def publish_with_recovery(self, documents: List[Document]) -> bool:
        """Publish with automatic recovery."""
        try:
            return self.publish_batch(documents)
        except Exception as e:
            logger.error(f"Publishing failed after retries: {e}")
            return False