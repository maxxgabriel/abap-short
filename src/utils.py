"""
Utility functions for ETL system
Migrated from ABAP ZETL_MACROS and utility functions
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any


def generate_unique_id(prefix: str = '') -> str:
    """
    Generate a unique ID with optional prefix
    
    Args:
        prefix: Prefix for the ID
    
    Returns:
        Unique identifier string
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"{prefix}{timestamp}"


def calculate_percentage(numerator: Any, denominator: Any) -> float:
    """
    Calculate percentage
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
    
    Returns:
        Percentage as float, 0 if denominator is 0
    """
    if denominator == 0 or denominator is None:
        return 0.0
    return (float(numerator) / float(denominator)) * 100.0


def format_currency(amount: Decimal, currency: str = 'USD') -> str:
    """
    Format currency value
    
    Args:
        amount: Amount to format
        currency: Currency code
    
    Returns:
        Formatted currency string
    """
    return f"{currency} {amount:,.2f}"


def add_days_to_date(base_date: datetime, days: int) -> datetime:
    """
    Add days to a date
    
    Args:
        base_date: Base date
        days: Number of days to add
    
    Returns:
        New date
    """
    return base_date + timedelta(days=days)


def validate_mandatory_field(value: Any, field_name: str) -> bool:
    """
    Validate that a mandatory field is not empty
    
    Args:
        value: Field value
        field_name: Field name for error messages
    
    Returns:
        True if valid, False otherwise
    """
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return False
    return True


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format
    
    Args:
        seconds: Duration in seconds
    
    Returns:
        Formatted duration string
    """
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    
    return " ".join(parts)