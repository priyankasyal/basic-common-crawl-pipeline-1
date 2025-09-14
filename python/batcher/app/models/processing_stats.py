from dataclasses import dataclass

@dataclass
class ProcessingStats:
    """Processing statistics."""
    
    total_lines: int = 0
    valid_documents: int = 0
    published_batches: int = 0
    failed_batches: int = 0
    processing_time: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        total = self.published_batches + self.failed_batches
        return (self.published_batches / total * 100) if total > 0 else 0.0

