"""
ETL Helper Utilities
Additional utility functions converted from ZETL_MACROS.
"""
from typing import Any, Optional
from datetime import datetime, timedelta
import uuid


def validate_field(field_value: Any, field_name: str = "") -> bool:
    """
    Validate mandatory field is not empty.
    Replaces ABAP macro: validate_field.
    
    Args:
        field_value: Value to validate
        field_name: Optional field name for error messages
        
    Returns:
        True if valid, False otherwise
    """
    if field_value is None or field_value == "" or (isinstance(field_value, (list, dict)) and len(field_value) == 0):
        return False
    return True


def calculate_percentage(numerator: float, denominator: float) -> float:
    """
    Calculate percentage safely handling division by zero.
    Replaces ABAP macro: calculate_percentage.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        
    Returns:
        Percentage value (0.0 if denominator is 0)
    """
    if denominator > 0:
        return (numerator / denominator) * 100
    return 0.0


def generate_unique_id(prefix: str = "") -> str:
    """
    Generate unique identifier with optional prefix.
    Replaces ABAP macro: generate_unique_id.
    
    Args:
        prefix: Optional prefix for the ID
        
    Returns:
        Unique identifier string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8]
    return f"{prefix}{timestamp}{unique_suffix}"


def format_currency(amount: float, currency: str = "USD", decimals: int = 2) -> str:
    """
    Format currency amount.
    Replaces ABAP macro: format_currency.
    
    Args:
        amount: Amount to format
        currency: Currency code
        decimals: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    return f"{amount:,.{decimals}f} {currency}"


def add_days_to_date(base_date: datetime, days: int) -> datetime:
    """
    Add days to a date.
    Replaces ABAP macro: add_days_to_date.
    
    Args:
        base_date: Base datetime object
        days: Number of days to add (can be negative)
        
    Returns:
        New datetime object
    """
    return base_date + timedelta(days=days)


class ETLErrorHandler:
    """
    Context manager for standardized ETL error handling.
    Replaces ABAP macro: handle_etl_error.
    """
    
    def __init__(self, logger, step: str, error_message_prefix: str = ""):
        """
        Initialize error handler.
        
        Args:
            logger: ETLLogger instance
            step: ETL process step
            error_message_prefix: Optional prefix for error messages
        """
        self.logger = logger
        self.step = step
        self.error_message_prefix = error_message_prefix
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            error_msg = f"{self.error_message_prefix}: {str(exc_val)}" if self.error_message_prefix else str(exc_val)
            self.logger.log_etl_message(
                step=self.step,
                status='E',
                message=error_msg
            )
            # Return False to propagate the exception
            return False
        return True