from dataclasses import dataclass

@dataclass
class BatchRequest:
    """Request to process a batch."""
    
    filename: str
    crawl: str