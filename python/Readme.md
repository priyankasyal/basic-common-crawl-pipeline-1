# CommonCrawl Worker
✅ Implemented Features
- Added metrics to track basic counters
- Added size limits for file 

# CommonCrawl Batcher

Refactored the batcher to be able to run multiple crawlers concurrently for multiple indexes with simple deduplication. 
Currently it uses `ThreadPoolExecuter` for managing multiple threads where each thread processes each index. 
Deduplication is managed by a simple in-memory set to keep a track of visited urls and a lock is used to manage the thread-safety of the set across different threads. Longer term a redis sets can be used for scaling and ensuring that the deduplication is managed across multiple instances. 

✅ Implemented Features
- Make it possible to pass the version of the crawl as an argument
- Error handling when publishing to RabbitMQ
- Support for metrics
- Allows support for providing multiple crawls that can be processed by the batcher with deduplication support 

### Environment Variables

Remember to set the RabbitMQ connection string with the port mapping to the AMQP port (5672)
```bash
export RABBITMQ_CONNECTION_STRING=amqp://localhost:<AMQP-PORT>
```

### Running via Command Line

```bash
python batcher/app/run_batcher.py --crawls CC-MAIN-2024-30,CC-MAIN-2024-22
```


```bash
python worker.py --crawls CC-MAIN-2024-30,CC-MAIN-2024-22
```

## Monitoring

Metrics are available at `http://localhost:9000/metrics` for batcher

and `http://localhost:9001/metrics` for worker

### 🔹 Batcher Metrics

These metrics are emitted while the batcher processes **Common Crawl CDX indexes** and publishes documents in batches.

#### 📝 Processing Metrics
- **`batcher_lines_total{crawl_index}`** – Total number of lines processed from CDX files.  
- **`batcher_valid_documents_total{crawl_index}`** – Number of valid documents that passed filtering.  

#### 📦 Batch Metrics
- **`batcher_batches_published_total{crawl_index}`** – Number of batches successfully published to the queue.  
- **`batcher_batches_failed_total{crawl_index,error}`** – Number of batches that failed to publish, labeled by error type.  

#### 🌐 URL Metrics
- **`batcher_urls_processed_total{crawl_index}`** – Number of unique URLs processed.  
- **`batcher_urls_filtered_total{crawl_index,reason}`** – Number of URLs filtered out (e.g., invalid format, too short, too long, status code failures).  
- **`batcher_urls_duplicate_total{crawl_index}`** – Number of duplicate URLs encountered.  

#### 📂 CDX Chunk Metrics
- **`batcher_cdx_chunks_processed_total{crawl_index}`** – Number of CDX chunks processed.  
- **`batcher_cdx_chunks_failed_total{crawl_index,error}`** – Number of CDX chunks that failed to process, labeled by error.  

#### ⚡ Performance Metrics
- **`batcher_errors_total{error_type}`** – General processing errors, labeled by error type.  

#### 📊 Progress Monitoring
- **`batcher_file_progress{crawl}`** – Percentage progress for a given crawl’s index file.  
- **`batcher_total_index_lines{crawl}`** – Total number of lines in the index file.  
- **`batcher_processed_index_lines_total{crawl}`** – Number of index lines processed.  

---

### 🔹 Worker Metrics

Metrics exposed by the worker during **WARC file processing**:

- **`worker_batches_total`** – Total number of consumed batches.  
- **`worker_batches_created`** – Timestamp when the latest batch was created.  
- **`worker_total_records_total`** – Total number of WARC records processed.  
- **`worker_total_records_created`** – Timestamp when the latest record was processed.  
- **`worker_non_response_records_total`** – Number of non-response records filtered out.  
- **`worker_response_records_total`** – Number of valid response records processed.  
- **`worker_extraction_failed_total`** – Number of documents where text extraction failed.  
- **`worker_extraction_successful_total`** – Number of documents where text extraction succeeded.  
- **`worker_empty_text_total`** – Number of documents where extracted text was empty.  

## Testing
Some unit tests for testing batcher functionality have been added.
```bash
pytest tests/
```

## Architecture

The batcher follows a simple, layered architecture:

1. **Models**: Data structures
2. **Services**: Business logic for publisher and batcher
3. **Utils**: Document filtering logic for batcher and retry logic for publisher
4. **Exceptions**: Custom Exceptions
5. **Core**: Setting some basic config

## Error Handling

- **Validation Errors**: Invalid input files or parameters
- **Processing Errors**: Document processing failures
- **Publishing Errors**: Message queue failures (with retry)
- **Network Errors**: Connection issues (with retry)

## Configuration

All settings can be configured via environment variables
