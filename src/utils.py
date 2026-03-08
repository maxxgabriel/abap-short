"""
Utility Functions
Common utility functions for ETL framework
"""

from datetime import datetime
from typing import Optional
import uuid


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run ID
    
    Args:
        prefix: ID prefix
        
    Returns:
        Unique run ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}{timestamp}"


def generate_log_id(prefix: str = "LOG") -> str:
    """
    Generate unique log ID
    
    Args:
        prefix: ID prefix
        
    Returns:
        Unique log ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8]
    return f"{prefix}{timestamp}{unique_suffix}"


def generate_analytics_id(trans_id: str, prefix: str = "ANL") -> str:
    """
    Generate analytics record ID
    
    Args:
        trans_id: Transaction ID
        prefix: ID prefix
        
    Returns:
        Analytics ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}{trans_id}{timestamp[-6:]}"


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


def format_number(number: int) -> str:
    """
    Format number with thousand separators
    
    Args:
        number: Number to format
        
    Returns:
        Formatted number string
    """
    return f"{number:,}"


def calculate_percentage(part: float, total: float) -> float:
    """
    Calculate percentage
    
    Args:
        part: Part value
        total: Total value
        
    Returns:
        Percentage (0-100)
    """
    if total == 0:
        return 0.0
    return (part / total) * 100


def validate_date_range(from_date: str, to_date: str) -> bool:
    """
    Validate date range
    
    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        
    Returns:
        True if valid, False otherwise
    """
    try:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
        return from_dt <= to_dt
    except ValueError:
        return False