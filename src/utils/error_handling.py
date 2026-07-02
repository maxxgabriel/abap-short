"""
Error Handling Utilities

Provides error handling utilities and custom exceptions for ETL processes.
"""

from typing import Optional


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: Record ID that caused the error
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.format_message())
    
    def format_message(self) -> str:
        """Format error message with context."""
        parts = [self.message]
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        return " | ".join(parts)


class ExtractError(ETLError):
    """Exception raised during data extraction."""
    pass


class TransformError(ETLError):
    """Exception raised during data transformation."""
    pass


class LoadError(ETLError):
    """Exception raised during data loading."""
    pass


class ValidationError(ETLError):
    """Exception raised during data validation."""
    pass


def handle_etl_error(error: Exception, logger, step: str) -> bool:
    """
    Handle ETL error with logging.
    
    Args:
        error: Exception that occurred
        logger: ETLLogger instance
        step: ETL step where error occurred
        
    Returns:
        False (always returns False to indicate failure)
    """
    from src.utils.logging import log_etl_message
    
    error_message = str(error)
    if isinstance(error, ETLError):
        error_message = error.format_message()
    
    log_etl_message(
        logger=logger,
        step=step,
        status='E',
        message=f"{step} failed: {error_message}"
    )
    
    return False


def retry_on_failure(max_attempts: int = 3):
    """
    Decorator to retry function on failure.
    
    Args:
        max_attempts: Maximum number of retry attempts
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
            
        return wrapper
    return decorator