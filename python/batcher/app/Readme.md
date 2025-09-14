# CommonCrawl Batcher

A simple, robust batcher for processing CommonCrawl index files.

## Usage

### Command Line

```bash
python run_batcher.py --filename /path/to/index.csv --crawl CC-MAIN-2024-30
```

### Environment Variables

```bash
export BATCH_SIZE=50
export MAX_RETRIES=3
export PROMETHEUS_PORT=9000
export RABBITMQ_CONNECTION_STRING=amqp://localhost:<AMQPPORT>
```

## Monitoring

Metrics are available at `http://localhost:9000/metrics`:

- `batcher_total_lines`: Total lines processed
- `batcher_valid_documents`: Valid documents found
- `batcher_published_batches`: Successfully published batches
- `batcher_failed_batches`: Failed batches
- `batcher_file_progress`: File processing progress

## Testing

```bash
pytest tests/
```

## Architecture

The batcher follows a simple, layered architecture:

1. **Models**: Data structures (`Document`, `ProcessingStats`)
2. **Services**: Business logic (`Batcher`, `Publisher`)
3. **Utils**: Document filtering (`DocumentFilter`)

## Error Handling

- **Validation Errors**: Invalid input files or parameters
- **Processing Errors**: Document processing failures
- **Publishing Errors**: Message queue failures (with retry)
- **Network Errors**: Connection issues (with retry)

## Configuration

All settings can be configured via environment variables or command-line options.