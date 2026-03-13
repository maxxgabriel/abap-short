"""
Helper utility functions for ETL processing.
Replaces ABAP macro utility functions.
"""
from datetime import datetime, timedelta
from typing import Optional, Union
from decimal import Decimal
import uuid


def generate_unique_id(prefix: str = "ID") -> str:
    """
    Generate unique ID with prefix and timestamp.
    Replaces: generate_unique_id macro
    
    Args:
        prefix: ID prefix (e.g., 'ETL', 'LOG', 'ANL')
        
    Returns:
        Unique ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:14]
    return f"{prefix}{timestamp}"


def generate_uuid(prefix: str = "") -> str:
    """
    Generate UUID-based unique ID.
    
    Args:
        prefix: Optional prefix
        
    Returns:
        UUID string
    """
    unique_id = str(uuid.uuid4()).replace('-', '')[:20]
    return f"{prefix}{unique_id}" if prefix else unique_id


def calculate_percentage(numerator: Union[int, float], 
                        denominator: Union[int, float],
                        decimals: int = 2) -> float:
    """
    Calculate percentage safely.
    Replaces: calculate_percentage macro
    
    Args:
        numerator: Top value
        denominator: Bottom value
        decimals: Number of decimal places
        
    Returns:
        Percentage value or 0.0 if denominator is 0
    """
    if denominator == 0:
        return 0.0
    
    percentage = (numerator / denominator) * 100
    return round(percentage, decimals)


def format_currency(amount: Union[float, Decimal], 
                   currency: str = 'USD',
                   decimals: int = 2) -> str:
    """
    Format currency value.
    Replaces: format_currency macro
    
    Args:
        amount: Currency amount
        currency: Currency code
        decimals: Decimal places
        
    Returns:
        Formatted currency string
    """
    formatted_amount = f"{float(amount):,.{decimals}f}"
    return f"{currency} {formatted_amount}"


def add_days_to_date(date: datetime, days: int) -> datetime:
    """
    Add days to a date.
    Replaces: add_days_to_date macro
    
    Args:
        date: Base date
        days: Number of days to add (can be negative)
        
    Returns:
        New date
    """
    return date + timedelta(days=days)


def safe_divide(numerator: Union[int, float],
               denominator: Union[int, float],
               default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is 0.
    
    Args:
        numerator: Top value
        denominator: Bottom value
        default: Default return value
        
    Returns:
        Division result or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def truncate_string(text: Optional[str], max_length: int) -> str:
    """
    Truncate string to maximum length.
    
    Args:
        text: Input string
        max_length: Maximum length
        
    Returns:
        Truncated string
    """
    if not text:
        return ""
    
    return text[:max_length] if len(text) > max_length else text


def is_valid_date(date_value: Optional[datetime]) -> bool:
    """
    Validate date value.
    
    Args:
        date_value: Date to validate
        
    Returns:
        True if valid date
    """
    if not date_value:
        return False
    
    try:
        if isinstance(date_value, datetime):
            return True
        datetime.strptime(str(date_value), "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def coalesce(*values):
    """
    Return first non-None value.
    
    Args:
        *values: Variable number of values
        
    Returns:
        First non-None value or None
    """
    for value in values:
        if value is not None:
            return value
    return None