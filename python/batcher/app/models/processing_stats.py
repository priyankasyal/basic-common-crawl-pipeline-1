from dataclasses import dataclass


@dataclass
class ProcessingStats:
    """Processing statistics."""

    processing_time: float = 0.0
