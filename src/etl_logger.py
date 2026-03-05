"""
ETL Logger Module
Provides logging functionality for ETL processes with message and statistics logging.
"""
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum


class LogStatus(Enum):
    """Log status enumeration"""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process step enumeration"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


@dataclass
class LogEntry:
    """ETL log entry data structure"""
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
    created_at: Optional[datetime] = field(default_factory=datetime.now)


class ETLLogger:
    """
    ETL Logger class for logging messages and statistics.
    Replaces ABAP log_etl_message and log_etl_statistics macros.
    """
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        """
        Initialize ETL logger with run ID and log level.
        
        Args:
            etl_run_id: Unique ETL run identifier
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Configure Python logging
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create console handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        Log ETL message with optional statistics.
        Replaces both log_etl_message and log_etl_statistics ABAP macros.
        
        Args:
            step: Process step name (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        now = datetime.now()
        
        # Create log entry
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
            created_at=now
        )
        
        # Store log entry
        self.log_entries.append(log_entry)
        
        # Log to Python logger with appropriate level
        log_msg = self._format_log_message(log_entry)
        
        if status == LogStatus.ERROR.value:
            self.logger.error(log_msg)
        elif status == LogStatus.WARNING.value:
            self.logger.warning(log_msg)
        elif status == LogStatus.INFO.value:
            self.logger.info(log_msg)
        else:  # SUCCESS
            self.logger.info(log_msg)
    
    def log_etl_message(
        self,
        step: str,
        status: str,
        message: str
    ) -> None:
        """
        Log simple ETL message without statistics.
        Direct replacement for ABAP log_etl_message macro.
        
        Args:
            step: Process step name
            status: Status code
            message: Log message
        """
        self.log_message(
            step=step,
            status=status,
            message=message
        )
    
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
        Log ETL message with statistics.
        Direct replacement for ABAP log_etl_statistics macro.
        
        Args:
            step: Process step name
            status: Status code
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
            message: Log message
        """
        self.log_message(
            step=step,
            status=status,
            message=message,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error
        )
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list[LogEntry]:
        """
        Get all log entries for this ETL run.
        
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
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp[:14]}"
    
    def _format_log_message(self, log_entry: LogEntry) -> str:
        """
        Format log entry for output.
        
        Args:
            log_entry: Log entry to format
            
        Returns:
            Formatted log message
        """
        msg = f"[{log_entry.process_step}] [{log_entry.status}] {log_entry.message}"
        
        if log_entry.records_processed > 0:
            msg += (
                f" | Processed: {log_entry.records_processed}, "
                f"Success: {log_entry.records_success}, "
                f"Error: {log_entry.records_error}"
            )
        
        return msg


def create_logger(etl_run_id: str, log_level: str = "INFO") -> ETLLogger:
    """
    Factory function to create ETL logger instance.
    
    Args:
        etl_run_id: Unique ETL run identifier
        log_level: Logging level
        
    Returns:
        ETLLogger instance
    """
    return ETLLogger(etl_run_id=etl_run_id, log_level=log_level)