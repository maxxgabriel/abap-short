"""
Decorators for ETL logging and validation.
Replaces ABAP DEFINE macros with Python decorators.
"""
from functools import wraps
from typing import Callable, Any
import logging
from datetime import datetime


def log_etl_step(step: str):
    """
    Decorator to log ETL step execution.
    Replaces: log_etl_message macro
    
    Usage:
        @log_etl_step("EXTRACT")
        def extract_data(self):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            logger = getattr(self, 'logger', logging.getLogger(__name__))
            
            # Log start
            logger.info(f"Starting {step} - {func.__name__}")
            start_time = datetime.now()
            
            try:
                result = func(self, *args, **kwargs)
                
                # Log success
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Completed {step} - {func.__name__} in {duration:.2f}s")
                
                return result
                
            except Exception as e:
                # Log error
                duration = (datetime.now() - start_time).total_seconds()
                logger.error(f"Failed {step} - {func.__name__} after {duration:.2f}s: {str(e)}")
                raise
                
        return wrapper
    return decorator


def log_etl_statistics(step: str):
    """
    Decorator to log ETL statistics with record counts.
    Replaces: log_etl_statistics macro
    
    Expected return format: (result, stats_dict)
    stats_dict should contain: processed, success, error
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            logger = getattr(self, 'logger', logging.getLogger(__name__))
            
            result, stats = func(self, *args, **kwargs)
            
            processed = stats.get('processed', 0)
            success = stats.get('success', 0)
            error = stats.get('error', 0)
            message = stats.get('message', f'{step} completed')
            
            logger.info(
                f"{step} - Processed: {processed}, Success: {success}, Error: {error} - {message}"
            )
            
            return result
            
        return wrapper
    return decorator


def validate_fields(*field_names):
    """
    Decorator to validate mandatory fields.
    Replaces: validate_field macro
    
    Usage:
        @validate_fields('customer_id', 'product_id', 'amount')
        def process_record(self, record):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, record, *args, **kwargs):
            for field in field_names:
                if not record.get(field):
                    raise ValueError(f"Mandatory field '{field}' is missing or empty")
            
            return func(self, record, *args, **kwargs)
            
        return wrapper
    return decorator


def handle_etl_error(error_step: str, default_return=None):
    """
    Decorator for standardized error handling.
    Replaces: handle_etl_error macro
    
    Usage:
        @handle_etl_error("LOAD", default_return=False)
        def load_data(self):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            logger = getattr(self, 'logger', logging.getLogger(__name__))
            
            try:
                return func(self, *args, **kwargs)
                
            except Exception as e:
                logger.error(f"{error_step} error in {func.__name__}: {str(e)}")
                
                # Log to ETL logger if available
                if hasattr(self, 'etl_logger'):
                    self.etl_logger.log_message(
                        step=error_step,
                        status='E',
                        message=f"{func.__name__}: {str(e)}"
                    )
                
                if default_return is not None:
                    return default_return
                raise
                
        return wrapper
    return decorator


def retry_on_failure(max_attempts: int = 3, delay: float = 1.0):
    """
    Decorator to retry failed operations.
    
    Usage:
        @retry_on_failure(max_attempts=3, delay=2.0)
        def load_to_database(self, data):
            ...
    """
    import time
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
                    else:
                        raise last_exception
                        
        return wrapper
    return decorator