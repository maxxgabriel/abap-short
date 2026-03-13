"""
ETL Logger Module
Provides logging functionality for ETL process with run management.
"""

from datetime import datetime
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass, field


@dataclass
class LogEntry:
    """Data class for log entries."""
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


class ETLLogger:
    """
    Logger for ETL operations with structured logging.
    """
    
    # Status codes
    STATUS_SUCCESS = "S"
    STATUS_ERROR = "E"
    STATUS_WARNING = "W"
    STATUS_INFO = "I"
    
    # Process steps
    STEP_INIT = "INIT"
    STEP_EXTRACT = "EXTRACT"
    STEP_TRANSFORM = "TRANSFORM"
    STEP_LOAD = "LOAD"
    STEP_VALIDATE = "VALIDATE"
    STEP_COMPLETE = "COMPLETE"
    STEP_ERROR = "ERROR"
    
    def __init__(self, etl_run_id: str, config: Dict):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique ETL run identifier
            config: Configuration dictionary
        """
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_entries: List[LogEntry] = []
        self._log_counter = 0
        
        # Configure Python logging
        self._configure_logging()
    
    def _configure_logging(self) -> None:
        """Configure Python logging based on config."""
        log_config = self.config.get("logging", {})
        log_level = log_config.get("level", "INFO")
        log_format = log_config.get(
            "format",
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format
        )
        
        self.logger = logging.getLogger(f"ETL_{self.etl_run_id}")
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log ID
        """
        self._log_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"LOG{timestamp}{self._log_counter:04d}"
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message with structured information.
        
        Args:
            step: Process step name
            status: Status code (S/E/W/I)
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
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self.log_entries.append(log_entry)
        
        # Log to Python logging
        log_message = (
            f"[{step}] [{status}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error})"
        )
        
        if status == self.STATUS_ERROR:
            self.logger.error(log_message)
        elif status == self.STATUS_WARNING:
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)
        
        # Console output
        print(f"{log_entry.execution_time} | {step:12} | {status} | {message}")
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run ID
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> List[LogEntry]:
        """
        Get all log entries.
        
        Returns:
            List of log entries
        """
        return self.log_entries
    
    def display_summary(self) -> None:
        """Display summary of logged operations."""
        if not self.log_entries:
            print("No log entries to display")
            return
        
        print("\n" + "=" * 60)
        print("Log Summary")
        print("=" * 60)
        
        # Count by status
        status_counts = {
            self.STATUS_SUCCESS: 0,
            self.STATUS_ERROR: 0,
            self.STATUS_WARNING: 0,
            self.STATUS_INFO: 0
        }
        
        total_processed = 0
        total_success = 0
        total_error = 0
        
        for entry in self.log_entries:
            status_counts[entry.status] = status_counts.get(entry.status, 0) + 1
            total_processed += entry.records_processed
            total_success += entry.records_success
            total_error += entry.records_error
        
        print(f"Total Log Entries:    {len(self.log_entries)}")
        print(f"Success Messages:     {status_counts[self.STATUS_SUCCESS]}")
        print(f"Error Messages:       {status_counts[self.STATUS_ERROR]}")
        print(f"Warning Messages:     {status_counts[self.STATUS_WARNING]}")
        print(f"Info Messages:        {status_counts[self.STATUS_INFO]}")
        print(f"\nRecords Processed:    {total_processed}")
        print(f"Records Success:      {total_success}")
        print(f"Records Error:        {total_error}")
        print("=" * 60)