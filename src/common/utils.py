"""
Utility functions for ETL processing.
Replaces ABAP macros from ZETL_MACROS.
"""

from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Optional, Any
import uuid


def generate_unique_id(prefix: str) -> str:
    """
    Generate unique ID with prefix.
    Replaces: generate_unique_id macro
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = str(uuid.uuid4().hex)[:6]
    return f"{prefix}{timestamp}{unique_suffix}"


def calculate_percentage(numerator: float, denominator: float) -> Decimal:
    """
    Calculate percentage safely.
    Replaces: calculate_percentage macro
    """
    if denominator == 0:
        return Decimal("0.00")
    return Decimal(str((numerator / denominator) * 100)).quantize(Decimal("0.01"))


def validate_field(value: Any) -> bool:
    """
    Validate that field is not empty/None.
    Replaces: validate_field macro
    """
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    if isinstance(value, (int, float, Decimal)) and value == 0:
        return False
    return True


def format_currency(amount: Decimal, currency: str) -> str:
    """
    Format currency amount.
    Replaces: format_currency macro
    """
    return f"{amount:,.2f} {currency}"


def add_days_to_date(input_date: date, days: int) -> date:
    """
    Add days to a date.
    Replaces: add_days_to_date macro
    """
    return input_date + timedelta(days=days)


def calculate_duration(start_time: datetime, end_time: datetime) -> float:
    """Calculate duration in seconds between two timestamps."""
    if not start_time or not end_time:
        return 0.0
    delta = end_time - start_time
    return delta.total_seconds()


def validate_date_range(from_date: date, to_date: date) -> tuple[bool, str]:
    """
    Validate date range.
    Returns: (is_valid, error_message)
    """
    if from_date > to_date:
        return False, "From Date cannot be later than To Date"
    
    if to_date > date.today():
        return False, "To Date cannot be in the future"
    
    return True, ""


def safe_divide(numerator: Decimal, denominator: Decimal, 
                default: Decimal = Decimal("0.00")) -> Decimal:
    """Safely divide with default value for zero denominator."""
    if denominator == 0:
        return default
    return numerator / denominator


def truncate_string(text: str, max_length: int) -> str:
    """Truncate string to max length."""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."