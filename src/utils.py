"""
Utility functions for ETL system
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional
import hashlib


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run ID
    
    Args:
        prefix: ID prefix
        
    Returns:
        Unique ETL run ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:14]
    return f"{prefix}{timestamp}"


def generate_log_id(prefix: str = "LOG") -> str:
    """
    Generate unique log ID
    
    Args:
        prefix: ID prefix
        
    Returns:
        Unique log ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:14]
    return f"{prefix}{timestamp}"


def generate_analytics_id(trans_id: str, prefix: str = "ANL") -> str:
    """
    Generate unique analytics ID
    
    Args:
        trans_id: Transaction ID
        prefix: ID prefix
        
    Returns:
        Unique analytics ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[8:14]
    return f"{prefix}{trans_id}{timestamp}"


def calculate_duration_seconds(start_time: datetime, end_time: datetime) -> int:
    """
    Calculate duration in seconds between two timestamps
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp
        
    Returns:
        Duration in seconds
    """
    if not start_time or not end_time:
        return 0
    
    duration = end_time - start_time
    return int(duration.total_seconds())


def format_currency(amount: Decimal, currency: str = "USD") -> str:
    """
    Format currency amount
    
    Args:
        amount: Amount to format
        currency: Currency code
        
    Returns:
        Formatted currency string
    """
    return f"{currency} {amount:,.2f}"


def validate_required_fields(record: dict, required_fields: list) -> tuple[bool, Optional[str]]:
    """
    Validate that required fields are present and not empty
    
    Args:
        record: Record dictionary
        required_fields: List of required field names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    for field in required_fields:
        if field not in record or record[field] is None or record[field] == "":
            return False, f"Missing or empty required field: {field}"
    
    return True, None


def safe_divide(numerator: Decimal, denominator: Decimal, 
                default: Decimal = Decimal('0.00')) -> Decimal:
    """
    Safe division with default value for zero denominator
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if denominator is zero
        
    Returns:
        Division result or default value
    """
    if denominator == 0 or denominator is None:
        return default
    
    return numerator / denominator


def calculate_percentage(part: Decimal, total: Decimal, 
                        precision: int = 2) -> Decimal:
    """
    Calculate percentage with specified precision
    
    Args:
        part: Part value
        total: Total value
        precision: Decimal precision
        
    Returns:
        Percentage value
    """
    if total == 0:
        return Decimal('0.00')
    
    percentage = (part / total) * Decimal('100')
    return round(percentage, precision)


def hash_record(record: dict) -> str:
    """
    Generate hash for record (for deduplication)
    
    Args:
        record: Record dictionary
        
    Returns:
        MD5 hash string
    """
    record_str = str(sorted(record.items()))
    return hashlib.md5(record_str.encode()).hexdigest()


__all__ = [
    'generate_etl_run_id',
    'generate_log_id',
    'generate_analytics_id',
    'calculate_duration_seconds',
    'format_currency',
    'validate_required_fields',
    'safe_divide',
    'calculate_percentage',
    'hash_record'
]