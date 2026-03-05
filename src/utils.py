"""
Utility functions for ETL Logger module.
"""
from datetime import datetime
from typing import Optional


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate a unique ETL run ID.

    Args:
        prefix: Prefix for the run ID

    Returns:
        Unique ETL run ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"{prefix}{timestamp}"


def format_execution_date(date_str: Optional[str] = None) -> str:
    """
    Format date string for execution date field.

    Args:
        date_str: Optional date string in ISO format

    Returns:
        Formatted date string (YYYY-MM-DD)
    """
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return datetime.now().strftime("%Y-%m-%d")


def format_execution_time(time_str: Optional[str] = None) -> str:
    """
    Format time string for execution time field.

    Args:
        time_str: Optional time string in ISO format

    Returns:
        Formatted time string (HH:MM:SS)
    """
    if time_str:
        try:
            dt = datetime.fromisoformat(time_str)
            return dt.strftime("%H:%M:%S")
        except ValueError:
            pass

    return datetime.now().strftime("%H:%M:%S")


def validate_status_code(status: str, valid_codes: list) -> bool:
    """
    Validate status code against allowed values.

    Args:
        status: Status code to validate
        valid_codes: List of valid status codes

    Returns:
        True if valid, False otherwise
    """
    return status in valid_codes


def validate_process_step(step: str, valid_steps: list) -> bool:
    """
    Validate process step against allowed values.

    Args:
        step: Process step to validate
        valid_steps: List of valid process steps

    Returns:
        True if valid, False otherwise
    """
    return step in valid_steps