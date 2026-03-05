"""
Utility functions for ETL logging framework
"""

from datetime import datetime
import uuid
from typing import Dict, Any


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run ID.
    Migrated from ABAP generate_etl_run_id method.
    
    Args:
        prefix: Prefix for the ID
        
    Returns:
        Unique ETL run identifier
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8].upper()
    return f"{prefix}{timestamp}{unique_suffix}"


def format_log_message(
    step: str,
    status: str,
    message: str,
    records_processed: int = 0,
    records_success: int = 0,
    records_error: int = 0
) -> str:
    """
    Format log message for consistent output.
    
    Args:
        step: Process step
        status: Status code
        message: Log message
        records_processed: Total records processed
        records_success: Successfully processed records
        records_error: Records with errors
        
    Returns:
        Formatted log message string
    """
    stats = ""
    if records_processed > 0:
        stats = (
            f" | Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Error: {records_error}"
        )
    
    return f"[{step}][{status}] {message}{stats}"


def calculate_duration(start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """
    Calculate duration between two timestamps.
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp
        
    Returns:
        Dictionary with duration metrics
    """
    duration = end_time - start_time
    
    return {
        "total_seconds": duration.total_seconds(),
        "minutes": duration.total_seconds() / 60,
        "hours": duration.total_seconds() / 3600,
        "formatted": str(duration)
    }


def validate_log_entry(log_entry: Dict[str, Any]) -> bool:
    """
    Validate log entry structure.
    
    Args:
        log_entry: Log entry dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = [
        "log_id", "etl_run_id", "execution_timestamp",
        "process_step", "status"
    ]
    
    return all(field in log_entry for field in required_fields)