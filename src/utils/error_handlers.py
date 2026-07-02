"""
Error Handling Utilities
Reusable error handling and exception management functions.
"""

import sys
import traceback
from typing import Optional, Callable, Any
from functools import wraps


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(self, message: str, step: str = "", record_id: str = ""):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            step: ETL step where error occurred
            record_id: Record ID related to error
        """
        self.message = message
        self.step = step
        self.record_id = record_id
        super().__init__(self.message)
    
    def __str__(self):
        parts = [self.message]
        if self.step:
            parts.append(f"Step: {self.step}")
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        return " | ".join(parts)


class ExtractError(ETLError):
    """Exception for extraction errors."""
    pass


class TransformError(ETLError):
    """Exception for transformation errors."""
    pass


class LoadError(ETLError):
    """Exception for load errors."""
    pass


def handle_etl_error(logger: Any, step: str, operation: str):
    """
    Decorator for ETL error handling.
    
    Args:
        logger: Logger instance
        step: ETL step name
        operation: Operation description
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = f"{operation}: {str(e)}"
                logger.log_message(
                    step=step,
                    status='E',
                    message=error_msg
                )
                
                # Log full traceback for debugging
                logger.logger.error(f"Full traceback:\n{traceback.format_exc()}")
                
                # Re-raise as ETL error
                raise ETLError(error_msg, step=step) from e
        
        return wrapper
    return decorator


def safe_execute(func: Callable, default_value: Any = None, 
                 suppress_errors: bool = False) -> Any:
    """
    Safely execute function with error handling.
    
    Args:
        func: Function to execute
        default_value: Value to return on error
        suppress_errors: Whether to suppress exceptions
        
    Returns:
        Function result or default value
    """
    try:
        return func()
    except Exception as e:
        if not suppress_errors:
            raise
        return default_value


def get_error_details(exception: Exception) -> dict:
    """
    Extract error details from exception.
    
    Args:
        exception: Exception object
        
    Returns:
        Dictionary with error details
    """
    return {
        'error_type': type(exception).__name__,
        'error_message': str(exception),
        'traceback': traceback.format_exc()
    }