"""Resilience utilities for ETL pipeline."""
import time
import logging
from typing import Callable, Any, List, Type
from functools import wraps


class RetryStrategy:
    """Implements retry logic with exponential backoff."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        initial_backoff: float = 2.0,
        max_backoff: float = 60.0,
        backoff_multiplier: float = 2.0,
        retryable_exceptions: List[Type[Exception]] = None
    ):
        """Initialize retry strategy.
        
        Args:
            max_attempts: Maximum number of retry attempts
            initial_backoff: Initial backoff delay in seconds
            max_backoff: Maximum backoff delay in seconds
            backoff_multiplier: Multiplier for exponential backoff
            retryable_exceptions: List of exception types to retry
        """
        self.max_attempts = max_attempts
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.retryable_exceptions = retryable_exceptions or [Exception]
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_backoff(self, attempt: int) -> float:
        """Calculate backoff delay for given attempt.
        
        Args:
            attempt: Current attempt number (0-indexed)
            
        Returns:
            Backoff delay in seconds
        """
        backoff = self.initial_backoff * (self.backoff_multiplier ** attempt)
        return min(backoff, self.max_backoff)
    
    def should_retry(self, exception: Exception) -> bool:
        """Determine if exception should trigger a retry.
        
        Args:
            exception: Exception that occurred
            
        Returns:
            True if should retry, False otherwise
        """
        return any(isinstance(exception, exc_type) 
                  for exc_type in self.retryable_exceptions)
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function
            
        Returns:
            Result of function execution
            
        Raises:
            Last exception if all retries exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_attempts):
            try:
                self.logger.info(f"Attempt {attempt + 1}/{self.max_attempts}: "
                               f"Executing {func.__name__}")
                result = func(*args, **kwargs)
                
                if attempt > 0:
                    self.logger.info(f"Success on attempt {attempt + 1}")
                
                return result
                
            except Exception as e:
                last_exception = e
                
                if not self.should_retry(e):
                    self.logger.error(f"Non-retryable exception: {str(e)}")
                    raise
                
                if attempt < self.max_attempts - 1:
                    backoff = self.calculate_backoff(attempt)
                    self.logger.warning(
                        f"Attempt {attempt + 1} failed: {str(e)}. "
                        f"Retrying in {backoff:.2f} seconds..."
                    )
                    time.sleep(backoff)
                else:
                    self.logger.error(f"All {self.max_attempts} attempts failed")
        
        raise last_exception


def with_retry(config: dict):
    """Decorator for adding retry logic to functions.
    
    Args:
        config: Retry configuration dictionary
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        retry_config = config.get('retry', {})
        strategy = RetryStrategy(
            max_attempts=retry_config.get('max_attempts', 3),
            initial_backoff=retry_config.get('initial_backoff_seconds', 2.0),
            max_backoff=retry_config.get('max_backoff_seconds', 60.0),
            backoff_multiplier=retry_config.get('backoff_multiplier', 2.0)
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return strategy.execute_with_retry(func, *args, **kwargs)
        
        return wrapper
    
    return decorator