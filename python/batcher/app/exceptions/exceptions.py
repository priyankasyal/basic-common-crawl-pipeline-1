class BatcherError(Exception):
    """Base exception for batcher errors."""
    pass


class ConfigurationError(BatcherError):
    """Configuration-related errors."""
    pass


class ProcessingError(BatcherError):
    """Document processing errors."""
    pass


class PublishingError(BatcherError):
    """Message publishing errors."""
    pass


class ValidationError(BatcherError):
    """Data validation errors."""
    pass