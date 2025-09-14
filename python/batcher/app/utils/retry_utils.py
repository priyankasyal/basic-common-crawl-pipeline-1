import time
import random
from functools import wraps
from typing import Callable, Type, Tuple


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """Simple retry decorator."""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_attempts - 1:
                        # Calculate delay with jitter
                        current_delay = delay * (backoff_factor ** attempt)
                        jitter = random.uniform(0.1, 0.3) * current_delay
                        sleep_time = current_delay + jitter
                        
                        print(f"Attempt {attempt + 1} failed: {e}. Retrying in {sleep_time:.2f}s...")
                        time.sleep(sleep_time)
                    else:
                        print(f"All {max_attempts} attempts failed. Last error: {e}")
            
            raise last_exception
        
        return wrapper
    return decorator