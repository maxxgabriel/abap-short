"""
ID Generation Utilities
Functions for generating unique identifiers.
"""

from datetime import datetime
import uuid


def generate_unique_id(prefix: str = "") -> str:
    """
    Generate unique ID with timestamp.
    
    Args:
        prefix: Optional prefix for ID
        
    Returns:
        Unique ID string
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"{prefix}{timestamp}"


def generate_etl_run_id() -> str:
    """Generate unique ETL run ID."""
    return generate_unique_id(prefix="ETL")


def generate_log_id() -> str:
    """Generate unique log entry ID."""
    return generate_unique_id(prefix="LOG")


def generate_analytics_id(trans_id: str) -> str:
    """
    Generate analytics record ID.
    
    Args:
        trans_id: Original transaction ID
        
    Returns:
        Analytics ID
    """
    timestamp = datetime.now().strftime('%H%M%S')
    return f"ANL{trans_id}{timestamp}"


def generate_uuid() -> str:
    """Generate UUID4 string."""
    return str(uuid.uuid4())