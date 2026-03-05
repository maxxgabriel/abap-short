"""
Utility Functions
Common utility functions for ETL processes.
"""

from datetime import datetime
from typing import Optional


def generate_run_id(prefix: str = "ETL") -> str:
    """
    Generate a unique run ID with timestamp.

    Args:
        prefix: Prefix for the ID (ETL, LOG, ANL)

    Returns:
        str: Unique run ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:14]
    return f"{prefix}{timestamp}"


def calculate_duration(start_time: datetime, end_time: datetime) -> int:
    """
    Calculate duration between two timestamps in seconds.

    Args:
        start_time: Start timestamp
        end_time: End timestamp

    Returns:
        int: Duration in seconds
    """
    if not start_time or not end_time:
        return 0
    
    duration = (end_time - start_time).total_seconds()
    return int(duration)


def format_currency(amount: float, currency: str = "USD") -> str:
    """
    Format currency amount.

    Args:
        amount: Numeric amount
        currency: Currency code

    Returns:
        str: Formatted currency string
    """
    return f"{currency} {amount:,.2f}"


def validate_date_range(from_date: str, to_date: str) -> bool:
    """
    Validate date range.

    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)

    Returns:
        bool: True if valid, False otherwise
    """
    try:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
        
        if from_dt > to_dt:
            return False
        
        if to_dt > datetime.now():
            return False
        
        return True
    except ValueError:
        return False