"""
ETL Logger Module
Provides logging functionality for ETL processes with structured log entries.
"""

import logging
from datetime import datetime
from enum import Enum
from typing import Optional
from dataclasses import dataclass, field


class LogStatus(Enum):
    """Log status codes matching ABAP constants."""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process step identifiers."""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


@dataclass
class LogEntry:
    """Structured log entry for ETL operations."""
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
    """
    ETL Logger implementation providing structured logging with run tracking.
    
    Attributes:
        etl_run_id: Unique identifier for the ETL run
        logger: Python logging instance
    """
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        """
        Initialize ETL Logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(f"ETLLogger.{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Configure console handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
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
        Log a message with ETL context.
        
        Args:
            step: Process step identifier (use ProcessStep enum)
            status: Log status (use LogStatus enum)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y-%m-%d'),
            execution_time=now.strftime('%H:%M:%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            timestamp=now
        )
        
        # Log to console/file
        log_msg = self._format_log_message(log_entry)
        
        if status == LogStatus.ERROR.value:
            self.logger.error(log_msg)
        elif status == LogStatus.WARNING.value:
            self.logger.warning(log_msg)
        elif status == LogStatus.INFO.value:
            self.logger.info(log_msg)
        else:  # SUCCESS
            self.logger.info(log_msg)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID based on timestamp."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp[:14]}"
    
    def _format_log_message(self, entry: LogEntry) -> str:
        """Format log entry for output."""
        msg = f"[{entry.process_step}] {entry.message}"
        if entry.records_processed > 0:
            msg += f" | Processed: {entry.records_processed}, Success: {entry.records_success}, Error: {entry.records_error}"
        return msg
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run identifier."""
        return self.etl_run_id


def create_logger(etl_run_id: Optional[str] = None, log_level: str = "INFO") -> ETLLogger:
    """
    Factory function to create an ETL logger instance.
    
    Args:
        etl_run_id: Optional ETL run ID, generates one if not provided
        log_level: Logging level
        
    Returns:
        Configured ETLLogger instance
    """
    if etl_run_id is None:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        etl_run_id = f"ETL{timestamp}"
    
    return ETLLogger(etl_run_id, log_level)