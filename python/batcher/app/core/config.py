import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Application configuration."""
    
    # RabbitMQ settings
    queue_name: str = os.getenv("QUEUE_NAME", "batches")
    
    # CommonCrawl settings
    commoncrawl_base_url: str = "https://data.commoncrawl.org"
    cluster_idx_filename: str = os.getenv("CLUSTER_IDX_FILENAME", "cluster.idx")
    
    # Processing settings
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    retry_delay: int = int(os.getenv("RETRY_DELAY", "1"))
    
    # Monitoring
    prometheus_port: int = int(os.getenv("PROMETHEUS_PORT", "9000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
# Global config instance
config = Config()