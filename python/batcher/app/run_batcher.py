#!/usr/bin/env python3
"""Entry point for the batcher service."""

import logging

import click
from core.config import config
from models import BatchRequest
from prometheus_client import REGISTRY, start_http_server
from services.batcher import Batcher


@click.command()
@click.option("--crawls", required=True, help="Comma-separated list of crawl versions")
def main(crawls):
    """Process CommonCrawl index file with multiple crawls using threading."""

    # Parse crawls
    crawl_list = [crawl.strip() for crawl in crawls.split(",")]

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting multi-threaded batcher")
    logger.info(f"Crawls: {crawl_list}")
    logger.info(f"Threads: {config.max_threads}")

    # Start metrics server
    start_http_server(config.prometheus_port)
    logger.info(
        f"Metrics available at http://localhost:{config.prometheus_port}/metrics"
    )

    try:
        # Create request
        request = BatchRequest(crawls=crawl_list)

        # Process file
        service = Batcher()
        service.process_request(request)

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


def start_metrics_server(port):
    # Unregister default collectors if they exist
    collectors = list(REGISTRY._names_to_collectors.keys())
    for c in collectors:
        try:
            REGISTRY.unregister(REGISTRY._names_to_collectors[c])
        except KeyError:
            pass
    start_http_server(port)


if __name__ == "__main__":
    main()
