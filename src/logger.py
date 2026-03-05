"""
ETL Logger Module
Provides logging functionality for ETL processes.
"""

import logging
from typing import Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import json


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
    
    def to_dict(self):
        return asdict(self)


class ETLLogger:
    """
    Logger for ETL operations.
    
    Provides structured logging with ETL-specific fields and formats.
    """
    
    # Status codes
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Process steps
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, config: dict = None):
        """
        Initialize the ETL Logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            config: Logger configuration
        """
        self.etl_run_id = etl_run_id
        self.config = config or {}
        self._log_counter = 0
        
        # Configure Python logger
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup Python logging configuration."""
        log_level = self.config.get('level', 'INFO')
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Create logger
        self.logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level))
        
        # Console handler
        if self.config.get('console_output', True):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_level))
            console_handler.setFormatter(logging.Formatter(log_format))
            self.logger.addHandler(console_handler)
        
        # File handler
        log_path = self.config.get('log_path')
        if log_path:
            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(getattr(logging, log_level))
            file_handler.setFormatter(logging.Formatter(log_format))
            self.logger.addHandler(file_handler)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
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
    ):
        """
        Log an ETL message.
        
        Args:
            step: ETL process step
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
        
        # Log to Python logger
        log_message = self._format_log_message(log_entry)
        
        if status == self.STATUS_ERROR:
            self.logger.error(log_message)
        elif status == self.STATUS_WARNING:
            self.logger.warning(log_message)
        elif status == self.STATUS_INFO:
            self.logger.info(log_message)
        else:
            self.logger.info(log_message)
        
        # Store for potential DB logging
        if self.config.get('db_logging', False):
            self._store_to_db(log_entry)
    
    def _format_log_message(self, log_entry: LogEntry) -> str:
        """Format log entry as string."""
        parts = [
            f"[{log_entry.process_step}]",
            f"Status: {log_entry.status}",
        ]
        
        if log_entry.records_processed > 0:
            parts.append(
                f"Records: {log_entry.records_processed} "
                f"(Success: {log_entry.records_success}, Error: {log_entry.records_error})"
            )
        
        parts.append(log_entry.message)
        
        return " | ".join(parts)
    
    def _store_to_db(self, log_entry: LogEntry):
        """
        Store log entry to database.
        This is a placeholder for actual DB implementation.
        """
        # In production, implement actual database logging
        pass
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.etl_run_id