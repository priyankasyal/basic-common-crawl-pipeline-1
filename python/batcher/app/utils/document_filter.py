import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from batcher.app.models import Document
from typing import Dict, Any

class DocumentFilter:
    
    @staticmethod
    def is_valid(document: Document) -> bool:
        metadata = document.metadata
        
        # Must have language information
        if "languages" not in metadata:
            return False
        
        # Must be English
        if "eng" not in metadata["languages"]:
            return False
        
        # Must have successful status
        if metadata.get("status") != "200":
            return False
        
        return True
    
    @staticmethod
    def get_rejection_reason(document: Document) -> str:
        """Get the reason why a document was rejected."""
        metadata = document.metadata
        
        if "languages" not in metadata:
            return "no_language"
        elif "eng" not in metadata["languages"]:
            return "non_english"
        elif metadata.get("status") != "200":
            return "non_200_status"
        else:
            return "unknown"