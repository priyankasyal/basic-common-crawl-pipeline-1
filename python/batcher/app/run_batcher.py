#!/usr/bin/env python3
"""Entry point for the batcher service."""

import click
import logging

from prometheus_client import start_http_server, REGISTRY
from services.batcher import Batcher
from models import BatchRequest
from core.config import config


@click.command()
@click.option("--filename", required=True, help="Cluster index filename")
@click.option("--crawl", required=True, help="Crawl version")
def main(filename, crawl):
    """Process CommonCrawl index file."""
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting batcher with crawl: {crawl}")
    
    # Start metrics server
    start_metrics_server(config.prometheus_port)
    logger.info(f"Metrics available at http://localhost:{config.prometheus_port}/metrics")
    
    try:
        # Create request
        request = BatchRequest(
            filename=filename,
            crawl=crawl
        )
        
        # Process file
        service = Batcher()
        stats = service.process_file(request)
        
        # Print results
        click.echo("Processing completed!")
        click.echo(f"Total lines: {stats.total_lines}")
        click.echo(f"Valid documents: {stats.valid_documents}")
        click.echo(f"Published batches: {stats.published_batches}")
        click.echo(f"Failed batches: {stats.failed_batches}")
        click.echo(f"Success rate: {stats.success_rate:.1f}%")
        click.echo(f"Processing time: {stats.processing_time:.2f}s")
        
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
