"""
ETL Exception Handler Utilities

Provides utilities for exception handling, logging, and retry logic.
"""

from typing import Callable, Optional, Any, Type, Tuple
from functools import wraps
import logging
import time

from src.exceptions.etl_exceptions import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError
)


class ExceptionHandler:
    """
    Utility class for handling ETL exceptions with retry logic.
    """
    
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        backoff_factor: float = 2.0
    ):
        """
        Initialize exception handler.
        
        Args:
            logger: Logger instance for logging errors
            max_retries: Maximum number of retry attempts
            retry_delay: Initial delay between retries in seconds
            backoff_factor: Exponential backoff multiplier
        """
        self.logger = logger or logging.getLogger(__name__)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
    
    def with_retry(
        self,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        fatal_exceptions: Tuple[Type[Exception], ...] = ()
    ):
        """
        Decorator for adding retry logic to functions.
        
        Args:
            retryable_exceptions: Exceptions that should trigger retry
            fatal_exceptions: Exceptions that should not be retried
            
        Returns:
            Decorated function with retry logic
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                last_exception = None
                delay = self.retry_delay
                
                for attempt in range(self.max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except fatal_exceptions as e:
                        self.logger.error(
                            f"Fatal error in {func.__name__}: {str(e)}"
                        )
                        raise
                    except retryable_exceptions as e:
                        last_exception = e
                        
                        if attempt < self.max_retries:
                            self.logger.warning(
                                f"Attempt {attempt + 1}/{self.max_retries + 1} "
                                f"failed for {func.__name__}: {str(e)}. "
                                f"Retrying in {delay:.2f} seconds..."
                            )
                            time.sleep(delay)
                            delay *= self.backoff_factor
                        else:
                            self.logger.error(
                                f"All {self.max_retries + 1} attempts failed "
                                f"for {func.__name__}"
                            )
                
                # If we get here, all retries failed
                raise last_exception
            
            return wrapper
        return decorator
    
    def log_exception(
        self,
        exception: ETLError,
        log_level: int = logging.ERROR
    ):
        """
        Log ETL exception with formatted message.
        
        Args:
            exception: ETL exception to log
            log_level: Logging level to use
        """
        self.logger.log(
            log_level,
            exception.get_formatted_message(),
            extra=exception.to_dict()
        )
    
    def handle_exception(
        self,
        exception: Exception,
        operation: str,
        record_id: Optional[str] = None,
        reraise: bool = True
    ) -> Optional[ETLError]:
        """
        Handle exception with logging and optional conversion to ETLError.
        
        Args:
            exception: Exception to handle
            operation: Operation where exception occurred
            record_id: Record ID if applicable
            reraise: Whether to reraise the exception
            
        Returns:
            ETLError if not reraised, None otherwise
            
        Raises:
            ETLError: If reraise is True
        """
        if isinstance(exception, ETLError):
            etl_error = exception
        else:
            # Convert to appropriate ETL exception
            operation_lower = operation.lower()
            if "extract" in operation_lower:
                etl_error = ExtractError(
                    message=f"Error during {operation}: {str(exception)}",
                    record_id=record_id,
                    previous=exception
                )
            elif "transform" in operation_lower:
                etl_error = TransformError(
                    message=f"Error during {operation}: {str(exception)}",
                    record_id=record_id,
                    previous=exception
                )
            elif "load" in operation_lower:
                etl_error = LoadError(
                    message=f"Error during {operation}: {str(exception)}",
                    record_id=record_id,
                    previous=exception
                )
            else:
                etl_error = ETLError(
                    message=f"Error during {operation}: {str(exception)}",
                    error_step=operation,
                    record_id=record_id,
                    previous=exception
                )
        
        self.log_exception(etl_error)
        
        if reraise:
            raise etl_error
        
        return etl_error


def safe_execute(
    func: Callable,
    error_type: Type[ETLError] = ETLError,
    error_message: Optional[str] = None,
    **error_kwargs
) -> Callable:
    """
    Decorator for safe execution with automatic exception conversion.
    
    Args:
        func: Function to execute safely
        error_type: Type of ETL error to raise
        error_message: Custom error message template
        **error_kwargs: Additional keyword arguments for error constructor
        
    Returns:
        Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ETLError:
            # Re-raise ETL errors as-is
            raise
        except Exception as e:
            # Convert to specified ETL error type
            message = error_message or f"Error in {func.__name__}: {str(e)}"
            raise error_type(
                message=message,
                previous=e,
                **error_kwargs
            )
    
    return wrapper