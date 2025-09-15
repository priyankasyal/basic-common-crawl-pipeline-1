import io
import json
import time

import trafilatura
from prometheus_client import Counter, start_http_server
from warcio.archiveiterator import WARCIterator

from common.commoncrawl import BASE_URL, CCDownloader, Downloader
from common.rabbitmq import QUEUE_NAME, rabbitmq_channel

total_batches_counter = Counter(
    "worker_batches_processed_total", "Number of batches processed"
)
total_documents_counter = Counter(
    "worker_documents_processed_total", "Documents processed"
)
total_text_extracted_counter = Counter(
    "worker_text_extracted_total", "Documents with text extracted"
)
total_processing_errors_counter = Counter(
    "worker_processing_errors_total", "Processing errors", ["error_type"]
)
total_warc_records_processed_counter = Counter(
    "worker_warc_records_processed_total", "WARC records processed"
)
total_documents_filtered_counter = Counter(
    "worker_documents_filtered_total", "Documents filtered by length", ["filter_reason"]
)

MIN_DOC_SIZE = 500
MAX_DOC_SIZE = 100000
channel = None


def _is_valid_document_size(text: str) -> bool:
    """Check if document length is within acceptable limits."""
    text_length = len(text)

    if text_length < MIN_DOC_SIZE:
        total_documents_filtered_counter.labels(filter_reason="too_small").inc()
        return False

    if text_length > MAX_DOC_SIZE:
        total_documents_filtered_counter.labels(filter_reason="too_big").inc()
        return False

    return True


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        for record in WARCIterator(io.BytesIO(data)):
            total_warc_records_processed_counter.inc()
            if record.rec_type == "response":
                try:
                    content = record.content_stream().read()
                    extracted_text = trafilatura.extract(content)
                    if extracted_text:
                        total_text_extracted_counter.inc()
                        # Check document size filter
                        if not _is_valid_document_size(extracted_text):
                            continue
                        total_documents_counter.inc()
                except Exception as e:
                    total_processing_errors_counter.labels(
                        error_type=type(e).__name__
                    ).inc()
                    print(f"Error processing record: {e}")
    total_batches_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    while True:
        try:
            channel = rabbitmq_channel()
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=lambda ch, method, properties, body: process_batch(
                    downloader, ch, method, properties, body
                ),
            )
            channel.start_consuming()

        except Exception:
            # Wait before reconnecting
            time.sleep(10)
            # Recreate the channel connection
            channel = rabbitmq_channel()


if __name__ == "__main__":
    main()
