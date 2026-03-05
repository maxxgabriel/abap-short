"""
ETL Logger Module

This module provides comprehensive logging functionality for the ETL system,
migrated from ABAP ZCL_ETL_LOGGER.
"""

import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict
import uuid

from src.constants import ETLConstants


@dataclass
class LogEntry:
    """Data class representing a log entry"""
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
    timestamp: Optional[str] = None
    
    def to_dict(self):
        """Convert log entry to dictionary"""
        return asdict(self)


class ETLLogger:
    """
    ETL Logger class for comprehensive logging functionality.
    Migrated from ABAP ZCL_ETL_LOGGER.
    """
    
    def __init__(
        self,
        etl_run_id: str,
        log_level: str = "INFO",
        log_format: Optional[str] = None
    ):
        """
        Initialize ETL Logger
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_format: Custom log format string
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Setup Python logger
        self._setup_logger(log_level, log_format)
    
    def _setup_logger(self, log_level: str, log_format: Optional[str]):
        """Setup Python logging configuration"""
        if log_format is None:
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        self.logger = logging.getLogger(f"ETL.{self.etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_level.upper()))
            formatter = logging.Formatter(log_format)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> LogEntry:
        """
        Log a message with ETL context
        
        Args:
            step: Process step identifier
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            
        Returns:
            LogEntry object
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
            timestamp=now.isoformat()
        )
        
        # Store log entry
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        self._log_to_python_logger(log_entry)
        
        return log_entry
    
    def _log_to_python_logger(self, log_entry: LogEntry):
        """Log entry to Python logging system"""
        log_msg = (
            f"[{log_entry.process_step}] {log_entry.message} "
            f"(Processed: {log_entry.records_processed}, "
            f"Success: {log_entry.records_success}, "
            f"Error: {log_entry.records_error})"
        )
        
        status_level_map = {
            ETLConstants.STATUS.SUCCESS: logging.INFO,
            ETLConstants.STATUS.INFO: logging.INFO,
            ETLConstants.STATUS.WARNING: logging.WARNING,
            ETLConstants.STATUS.ERROR: logging.ERROR,
        }
        
        level = status_level_map.get(log_entry.status, logging.INFO)
        self.logger.log(level, log_msg)
    
    @staticmethod
    def _generate_log_id() -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_part = str(uuid.uuid4())[:6]
        return f"{ETLConstants.PREFIX.LOG_ID}{timestamp}{unique_part}"
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries for this run"""
        return self.log_entries
    
    def get_statistics(self) -> dict:
        """Get logging statistics"""
        total_processed = sum(e.records_processed for e in self.log_entries)
        total_success = sum(e.records_success for e in self.log_entries)
        total_error = sum(e.records_error for e in self.log_entries)
        
        status_counts = {}
        for entry in self.log_entries:
            status_counts[entry.status] = status_counts.get(entry.status, 0) + 1
        
        return {
            'total_log_entries': len(self.log_entries),
            'total_records_processed': total_processed,
            'total_records_success': total_success,
            'total_records_error': total_error,
            'status_counts': status_counts
        }
    
    def clear_logs(self):
        """Clear all stored log entries"""
        self.log_entries.clear()