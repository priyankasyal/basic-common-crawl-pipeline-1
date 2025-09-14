from dataclasses import dataclass
from typing import List

@dataclass
class BatchRequest:
    """Request to process a batch."""
    
    crawls: List[str]  