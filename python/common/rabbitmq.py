import logging
import os
from abc import ABC, abstractmethod

import pika

logger = logging.getLogger(__name__)


QUEUE_NAME = "batches"


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self.channel = rabbitmq_channel()

    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
        )


def rabbitmq_channel() -> pika.adapters.blocking_connection.BlockingChannel:
    """Create a RabbitMQ channel with proper queue handling."""

    connection_string = os.environ.get("RABBITMQ_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("RABBITMQ_CONNECTION_STRING environment variable is required")

    try:
        connection = pika.BlockingConnection(pika.URLParameters(connection_string))
        channel = connection.channel()

        # Check if queue exists first, then declare with appropriate parameters
        try:
            # Try to check if queue exists (passive=True)
            channel.queue_declare(queue=QUEUE_NAME, passive=True)
            logger.info(f"Queue '{QUEUE_NAME}' already exists")
        except pika.exceptions.AMQPChannelError:
            # Queue doesn't exist, create it as durable
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            logger.info(f"Created durable queue '{QUEUE_NAME}'")

        return channel

    except Exception as e:
        logger.error(f"Failed to create RabbitMQ channel: {e}")
        raise
