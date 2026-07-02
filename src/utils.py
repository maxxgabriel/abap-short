"""
Utility functions for ETL system.
"""
from datetime import datetime
from typing import Optional


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run identifier.
    
    Args:
        prefix: ID prefix (default: ETL)
        
    Returns:
        Unique run ID with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}{timestamp}"


def generate_log_id(prefix: str = "LOG") -> str:
    """
    Generate unique log entry identifier.
    
    Args:
        prefix: ID prefix (default: LOG)
        
    Returns:
        Unique log ID with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:14]
    return f"{prefix}{timestamp}"


def generate_analytics_id(trans_id: str, prefix: str = "ANL") -> str:
    """
    Generate unique analytics record identifier.
    
    Args:
        trans_id: Source transaction ID
        prefix: ID prefix (default: ANL)
        
    Returns:
        Unique analytics ID
    """
    timestamp = datetime.now().strftime("%H%M%S")
    return f"{prefix}{trans_id}{timestamp}"


def format_duration(start_time: datetime, end_time: Optional[datetime] = None) -> str:
    """
    Format duration between timestamps.
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp (default: now)
        
    Returns:
        Formatted duration string
    """
    if end_time is None:
        end_time = datetime.now()
    
    duration = (end_time - start_time).total_seconds()
    
    hours = int(duration // 3600)
    minutes = int((duration % 3600) // 60)
    seconds = int(duration % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"