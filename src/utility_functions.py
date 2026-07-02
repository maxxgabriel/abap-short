"""
ETL Utility Functions Module
Collection of utility functions converted from ABAP macros.
"""
from datetime import datetime, timedelta
from typing import Any, Optional
from decimal import Decimal


def validate_field(value: Any) -> bool:
    """
    Validate if a field has a value.
    Replaces ABAP validate_field macro.
    
    Args:
        value: Value to validate
        
    Returns:
        True if value is valid (not None, not empty), False otherwise
    """
    if value is None:
        return False
    
    if isinstance(value, str) and value.strip() == "":
        return False
    
    if isinstance(value, (list, dict, tuple)) and len(value) == 0:
        return False
    
    return True


def calculate_percentage(
    numerator: float,
    denominator: float,
    default: float = 0.0
) -> float:
    """
    Calculate percentage with division by zero protection.
    Replaces ABAP calculate_percentage macro.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value if denominator is zero
        
    Returns:
        Calculated percentage
    """
    if denominator > 0:
        return (numerator / denominator) * 100
    return default


def generate_unique_id(prefix: str = "") -> str:
    """
    Generate unique ID with optional prefix.
    Replaces ABAP generate_unique_id macro.
    
    Args:
        prefix: Optional prefix for the ID
        
    Returns:
        Unique identifier string
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    unique_id = timestamp[:14]
    
    if prefix:
        return f"{prefix}{unique_id}"
    return unique_id


def format_currency(
    amount: float,
    currency: str = "USD",
    decimal_places: int = 2
) -> str:
    """
    Format amount as currency string.
    Replaces ABAP format_currency macro.
    
    Args:
        amount: Amount to format
        currency: Currency code
        decimal_places: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    formatted_amount = f"{amount:,.{decimal_places}f}"
    return f"{formatted_amount} {currency}"


def add_days_to_date(
    base_date: datetime,
    days: int
) -> datetime:
    """
    Add days to a date.
    Replaces ABAP add_days_to_date macro.
    
    Args:
        base_date: Base date
        days: Number of days to add
        
    Returns:
        Calculated date
    """
    return base_date + timedelta(days=days)


def handle_etl_error(
    logger: Any,
    error: Exception,
    step: str,
    context: str = ""
) -> bool:
    """
    Handle ETL errors with logging.
    Replaces ABAP handle_etl_error macro.
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        step: Process step where error occurred
        context: Additional context information
        
    Returns:
        False (to indicate failure)
    """
    error_msg = f"{context}: {str(error)}" if context else str(error)
    
    logger.log_message(
        step=step,
        status='E',
        message=error_msg
    )
    
    return False


def safe_divide(
    numerator: float,
    denominator: float,
    default: float = 0.0
) -> float:
    """
    Safely divide two numbers with default for division by zero.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value if denominator is zero
        
    Returns:
        Division result or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def safe_decimal_divide(
    numerator: Decimal,
    denominator: Decimal,
    default: Decimal = Decimal('0.0')
) -> Decimal:
    """
    Safely divide two Decimal numbers with default for division by zero.
    
    Args:
        numerator: Numerator value as Decimal
        denominator: Denominator value as Decimal
        default: Default value if denominator is zero
        
    Returns:
        Division result or default as Decimal
    """
    if denominator == 0:
        return default
    return numerator / denominator


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate string to maximum length with optional suffix.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    
    truncate_at = max_length - len(suffix)
    return text[:truncate_at] + suffix


def coalesce(*args: Any) -> Any:
    """
    Return first non-None value from arguments.
    
    Args:
        *args: Variable number of arguments
        
    Returns:
        First non-None value or None if all are None
    """
    for arg in args:
        if arg is not None:
            return arg
    return None


def is_valid_date_range(from_date: datetime, to_date: datetime) -> bool:
    """
    Validate date range is valid (from <= to).
    
    Args:
        from_date: Start date
        to_date: End date
        
    Returns:
        True if valid range, False otherwise
    """
    return from_date <= to_date


def batch_iterable(iterable: list, batch_size: int):
    """
    Generator to process iterable in batches.
    
    Args:
        iterable: List to batch
        batch_size: Size of each batch
        
    Yields:
        Batch of items
    """
    for i in range(0, len(iterable), batch_size):
        yield iterable[i:i + batch_size]