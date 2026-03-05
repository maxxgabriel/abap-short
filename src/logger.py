"""
Logging module for Sales ETL.
Provides structured logging for ETL process.
Migrated from ABAP ZCL_ETL_LOGGER class.
"""

from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field

from src.config import ProcessStep, StatusCode, IDPrefix


@dataclass
class LogEntry:
    """Structure for ETL log entry."""
    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


class ETLLogger:
    """Logger for ETL process."""
    
    def __init__(self, etl_run_id: str):
        """
        Initialize logger.
        
        Args:
            etl_run_id: Unique identifier for ETL run
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
    
    def log_message(
        self,
        step: ProcessStep,
        status: StatusCode,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """
        Log a message for the ETL process.
        
        Args:
            step: Process step
            status: Status code
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime("%Y-%m-%d"),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step.value,
            status=status.value,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            timestamp=now
        )
        
        self.log_entries.append(log_entry)
        
        # Console output
        status_str = self._format_status(status)
        print(f"{log_entry.execution_time} | {log_entry.process_step:12} | "
              f"{status_str} | {message}")
        
        if records_processed > 0:
            print(f"  -> Processed: {records_processed}, "
                  f"Success: {records_success}, "
                  f"Error: {records_error}")
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries.
        
        Returns:
            List of log entries
        """
        return self.log_entries
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{IDPrefix.LOG_ID}{timestamp}"
    
    @staticmethod
    def _format_status(status: StatusCode) -> str:
        """
        Format status for display.
        
        Args:
            status: Status code
            
        Returns:
            Formatted status string
        """
        status_colors = {
            StatusCode.SUCCESS: "✓ SUCCESS",
            StatusCode.ERROR: "✗ ERROR  ",
            StatusCode.WARNING: "⚠ WARNING",
            StatusCode.INFO: "ℹ INFO   ",
            StatusCode.NEW: "○ NEW    ",
            StatusCode.PROCESSED: "● PROCESS"
        }
        return status_colors.get(status, status.value)