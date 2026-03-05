"""
Utility functions for ETL processing.
Migrated from ABAP includes ZETL_MACROS and helper methods.
"""

import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from src.config import PREFIXES


def generate_unique_id(prefix: str = "") -> str:
    """
    Generate unique ID with optional prefix.
    Migrated from ZETL_MACROS generate_unique_id.
    
    Args:
        prefix: ID prefix (ETL, LOG, ANL, etc.)
    
    Returns:
        Unique identifier string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = str(uuid.uuid4())[:6]
    return f"{prefix}{timestamp}{unique_suffix}".upper()


def generate_etl_run_id() -> str:
    """Generate ETL run ID."""
    return generate_unique_id(PREFIXES.ETL_RUN)


def generate_log_id() -> str:
    """Generate log entry ID."""
    return generate_unique_id(PREFIXES.LOG_ID)


def generate_analytics_id(trans_id: str) -> str:
    """
    Generate analytics ID based on transaction ID.
    Migrated from ZCL_ETL_TRANSFORMER.calculate_analytics.
    """
    timestamp = datetime.now().strftime("%H%M%S")
    return f"{PREFIXES.ANALYTICS_ID}{trans_id}{timestamp}"


def calculate_percentage(numerator: float, denominator: float) -> float:
    """
    Calculate percentage safely.
    Migrated from ZETL_MACROS calculate_percentage.
    """
    if denominator > 0:
        return (numerator / denominator) * 100.0
    return 0.0


def add_days_to_date(base_date: str, days: int) -> str:
    """
    Add days to a date string.
    Migrated from ZETL_MACROS add_days_to_date.
    
    Args:
        base_date: Date in YYYY-MM-DD format
        days: Number of days to add (can be negative)
    
    Returns:
        New date in YYYY-MM-DD format
    """
    date_obj = datetime.strptime(base_date, "%Y-%m-%d")
    new_date = date_obj + timedelta(days=days)
    return new_date.strftime("%Y-%m-%d")


def format_currency(amount: Decimal, currency: str = "USD") -> str:
    """
    Format currency amount for display.
    Migrated from ZETL_MACROS format_currency.
    """
    return f"{currency} {amount:,.2f}"


def validate_mandatory_field(value: Optional[str]) -> bool:
    """
    Validate that mandatory field is not empty.
    Migrated from ZETL_MACROS validate_field.
    """
    return value is not None and str(value).strip() != ""


def calculate_duration_seconds(start_time: datetime, end_time: datetime) -> int:
    """Calculate duration in seconds between two timestamps."""
    duration = end_time - start_time
    return int(duration.total_seconds())


def format_execution_time() -> str:
    """
    Format current time for execution timestamp.
    Returns time in HH:MM:SS format for ABAP compatibility.
    """
    return datetime.now().strftime("%H:%M:%S")


def safe_decimal(value: any, default: Decimal = Decimal("0.00")) -> Decimal:
    """
    Safely convert value to Decimal.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
    
    Returns:
        Decimal value
    """
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return default


def truncate_string(text: str, max_length: int) -> str:
    """
    Truncate string to maximum length.
    Useful for ABAP char field compatibility.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."