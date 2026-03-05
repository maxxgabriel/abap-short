"""
Data Formatting Utilities

Provides formatting functions for currency, dates, and other data types.
"""

from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from typing import Optional


def format_currency(amount: float, currency_code: str, decimals: int = 2) -> str:
    """
    Format amount as currency string.
    
    Args:
        amount: Numeric amount
        currency_code: Currency code (USD, EUR, etc.)
        decimals: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    decimal_amount = Decimal(str(amount)).quantize(
        Decimal(10) ** -decimals,
        rounding=ROUND_HALF_UP
    )
    return f"{currency_code} {decimal_amount:,.{decimals}f}"


def format_date(date_obj: datetime, format_string: str = '%Y-%m-%d') -> str:
    """
    Format date object as string.
    
    Args:
        date_obj: Date object
        format_string: Format string
        
    Returns:
        Formatted date string
    """
    return date_obj.strftime(format_string)


def add_days_to_date(date_str: str, days: int, input_format: str = '%Y-%m-%d') -> str:
    """
    Add days to a date string.
    
    Args:
        date_str: Date string
        days: Number of days to add
        input_format: Input date format
        
    Returns:
        New date string in same format
    """
    date_obj = datetime.strptime(date_str, input_format)
    new_date = date_obj + timedelta(days=days)
    return new_date.strftime(input_format)


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format value as percentage string.
    
    Args:
        value: Percentage value (e.g., 15.5 for 15.5%)
        decimals: Number of decimal places
        
    Returns:
        Formatted percentage string
    """
    return f"{value:.{decimals}f}%"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (e.g., "1h 23m 45s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    
    return " ".join(parts)


def format_number(value: float, decimals: int = 2, thousands_sep: bool = True) -> str:
    """
    Format number with optional thousands separator.
    
    Args:
        value: Numeric value
        decimals: Number of decimal places
        thousands_sep: Include thousands separator
        
    Returns:
        Formatted number string
    """
    if thousands_sep:
        return f"{value:,.{decimals}f}"
    return f"{value:.{decimals}f}"