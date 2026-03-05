"""
ETL Utility Functions
Helper functions for ID generation, validation, and data conversion.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
import hashlib

from src.config import ETLConstants


def generate_etl_run_id() -> str:
    """
    Generate unique ETL run ID.
    Format: ETL + timestamp (YYYYMMDDHHMMSSffffff)
    
    Returns:
        Unique ETL run ID string
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"{ETLConstants.ID_PREFIXES.ETL_RUN}{timestamp}"


def generate_log_id() -> str:
    """
    Generate unique log entry ID.
    Format: LOG + timestamp (YYYYMMDDHHMMSSffffff)
    
    Returns:
        Unique log ID string
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"{ETLConstants.ID_PREFIXES.LOG_ID}{timestamp}"


def generate_analytics_id(trans_id: str, timestamp: Optional[datetime] = None) -> str:
    """
    Generate unique analytics record ID.
    Format: ANL + trans_id + timestamp
    
    Args:
        trans_id: Original transaction ID
        timestamp: Optional timestamp, uses current time if not provided
        
    Returns:
        Unique analytics ID string
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    ts_str = timestamp.strftime('%Y%m%d%H%M%S')
    return f"{ETLConstants.ID_PREFIXES.ANALYTICS_ID}{trans_id}{ts_str}"


def safe_decimal(value: any, default: Decimal = Decimal('0.00')) -> Decimal:
    """
    Safely convert value to Decimal with fallback.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Decimal value
    """
    try:
        if value is None:
            return default
        return Decimal(str(value))
    except (ValueError, TypeError):
        return default


def validate_currency(currency: str) -> bool:
    """
    Validate currency code.
    
    Args:
        currency: Currency code to validate
        
    Returns:
        True if valid, False otherwise
    """
    valid_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CNY']
    return currency in valid_currencies


def validate_quantity(quantity: int) -> bool:
    """
    Validate quantity is within acceptable range.
    
    Args:
        quantity: Quantity to validate
        
    Returns:
        True if valid, False otherwise
    """
    return 1 <= quantity <= 10000


def validate_unit_price(unit_price: Decimal) -> bool:
    """
    Validate unit price is within acceptable range.
    
    Args:
        unit_price: Unit price to validate
        
    Returns:
        True if valid, False otherwise
    """
    return Decimal('0.01') <= unit_price <= Decimal('999999.99')


def calculate_checksum(data: str) -> str:
    """
    Calculate MD5 checksum for data validation.
    
    Args:
        data: Data string to hash
        
    Returns:
        MD5 checksum hex string
    """
    return hashlib.md5(data.encode()).hexdigest()


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (HH:MM:SS)
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"