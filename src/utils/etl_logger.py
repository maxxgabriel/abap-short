"""
ETL Logger Utility Module
Converts ZETL_MACROS ABAP macros into Python utility functions with proper logger integration.
"""
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
import uuid


@dataclass
class LogEntry:
    """Structure for ETL log entries."""
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
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


class ETLLogger:
    """
    ETL Logger class providing structured logging functionality.
    Replaces ABAP macros: log_etl_message and log_etl_statistics.
    """
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        """
        Initialize ETL Logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            log_level: Python logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Configure handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log_etl_message(
        self,
        step: str,
        status: str,
        message: str
    ) -> None:
        """
        Log ETL message with timestamp.
        Replaces ABAP macro: log_etl_message.
        
        Args:
            step: ETL process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
        """
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=datetime.now().strftime("%Y-%m-%d"),
            execution_time=datetime.now().strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            message=message
        )
        
        self._write_log(log_entry)
    
    def log_etl_statistics(
        self,
        step: str,
        status: str,
        records_processed: int,
        records_success: int,
        records_error: int,
        message: str
    ) -> None:
        """
        Log ETL statistics with record counts.
        Replaces ABAP macro: log_etl_statistics.
        
        Args:
            step: ETL process step
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
            message: Log message text
        """
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=datetime.now().strftime("%Y-%m-%d"),
            execution_time=datetime.now().strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self._write_log(log_entry)
    
    def _write_log(self, log_entry: LogEntry) -> None:
        """
        Write log entry to logger with appropriate level.
        
        Args:
            log_entry: LogEntry object to write
        """
        log_msg = (
            f"[{log_entry.process_step}] "
            f"{log_entry.message}"
        )
        
        if log_entry.records_processed > 0:
            log_msg += (
                f" | Processed: {log_entry.records_processed}, "
                f"Success: {log_entry.records_success}, "
                f"Errors: {log_entry.records_error}"
            )
        
        # Map status codes to logging levels
        status_level_map = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }
        
        level = status_level_map.get(log_entry.status, logging.INFO)
        self.logger.log(level, log_msg)
        
        # Store for potential database persistence
        # In production, write to database table or log aggregation system
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_suffix = str(uuid.uuid4())[:8]
        return f"LOG{timestamp}{unique_suffix}"
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id


def create_etl_logger(etl_run_id: Optional[str] = None, log_level: str = "INFO") -> ETLLogger:
    """
    Factory function to create ETL logger instance.
    
    Args:
        etl_run_id: Optional ETL run ID. If not provided, generates one.
        log_level: Logging level
        
    Returns:
        Configured ETLLogger instance
    """
    if not etl_run_id:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        etl_run_id = f"ETL{timestamp}"
    
    return ETLLogger(etl_run_id=etl_run_id, log_level=log_level)


# Convenience functions for backward compatibility with macro-style usage
def log_message(logger: ETLLogger, step: str, status: str, message: str) -> None:
    """
    Convenience function matching ABAP macro signature.
    
    Args:
        logger: ETLLogger instance
        step: Process step
        status: Status code
        message: Log message
    """
    logger.log_etl_message(step=step, status=status, message=message)


def log_statistics(
    logger: ETLLogger,
    step: str,
    status: str,
    processed: int,
    success: int,
    error: int,
    message: str
) -> None:
    """
    Convenience function matching ABAP macro signature.
    
    Args:
        logger: ETLLogger instance
        step: Process step
        status: Status code
        processed: Records processed
        success: Successful records
        error: Error records
        message: Log message
    """
    logger.log_etl_statistics(
        step=step,
        status=status,
        records_processed=processed,
        records_success=success,
        records_error=error,
        message=message
    )