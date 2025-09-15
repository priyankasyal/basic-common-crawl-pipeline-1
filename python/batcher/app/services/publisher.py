"""Simple message publisher with retry logic."""

import json
import logging
import os
import sys
from typing import List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from batcher.app.core.config import config
from batcher.app.exceptions import PublishingError
from batcher.app.models import Document
from batcher.app.utils.retry_utils import retry
from common.rabbitmq import QUEUE_NAME

logger = logging.getLogger(__name__)


class Publisher:
    """Simple message publisher."""

    def __init__(self, channel):
        self.channel = channel

    @retry(
        max_attempts=config.max_retries,
        delay=config.retry_delay,
        exceptions=(Exception,),
    )
    def publish_batch(self, documents: List[Document]) -> bool:
        """Publish a batch of documents."""
        try:
            # Convert documents to JSON
            batch_data = [doc.to_dict() for doc in documents]
            message = json.dumps(batch_data)

            # Publish message
            self.channel.basic_publish(
                exchange="", routing_key=QUEUE_NAME, body=message
            )

            logger.info(f"Published batch of {len(documents)} documents")
            return True

        except Exception as e:
            logger.error(f"Failed to publish batch: {e}")
            raise PublishingError(f"Publishing failed: {e}")

    def publish_with_recovery(self, documents: List[Document]) -> bool:
        """Publish with automatic recovery."""
        try:
            return self.publish_batch(documents)
        except Exception as e:
            logger.error(f"Publishing failed after retries: {e}")
            return False
