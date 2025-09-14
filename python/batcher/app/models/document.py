from dataclasses import dataclass
from typing import Dict, Any, Optional
import json


@dataclass
class Document:
    
    surt_url: str
    timestamp: str
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "surt_url": self.surt_url,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_line(cls, line: str) -> Optional['Document']:
        """Create Document from a CommonCrawl index line."""
        if not line.strip():
            return None
        
        parts = line.split(" ", 2)
        if len(parts) < 3:
            return None
        
        try:
            metadata = json.loads(parts[2])
        except json.JSONDecodeError:
            return None
        
        return cls(
            surt_url=parts[0],
            timestamp=parts[1],
            metadata=metadata
        )