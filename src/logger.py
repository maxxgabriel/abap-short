"""
ETL Logger Module

Provides comprehensive logging functionality for ETL processes,
migrated from ABAP ZCL_ETL_LOGGER.
"""

import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, asdict
from pathlib import Path

from src.constants import ETLConstants


@dataclass
class LogEntry:
    """Log entry data structure"""
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
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)


class ETLLogger:
    """
    ETL logging utility class
    
    Provides structured logging with ETL-specific context including
    run IDs, process steps, record counts, and status tracking.
    """
    
    def __init__(
        self,
        etl_run_id: str,
        log_level: str = "INFO",
        log_file: Optional[str] = None
    ):
        """
        Initialize ETL logger
        
        Args:
            etl_run_id: Unique ETL run identifier
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional log file path
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Set up Python logger
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if specified
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
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
        Log a message with ETL context
        
        Args:
            step: ETL process step
            status: Status code (S/E/W/I)
            message: Log message
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
            message=message
        )
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_msg = self._format_message(log_entry)
        
        if status == ETLConstants.STATUS.ERROR:
            self.logger.error(log_msg)
        elif status == ETLConstants.STATUS.WARNING:
            self.logger.warning(log_msg)
        elif status == ETLConstants.STATUS.INFO:
            self.logger.info(log_msg)
        else:
            self.logger.info(log_msg)
    
    def _format_message(self, entry: LogEntry) -> str:
        """Format log entry as string"""
        msg = f"[{entry.process_step}] {entry.message}"
        if entry.records_processed > 0:
            msg += f" | Processed: {entry.records_processed}"
            msg += f" | Success: {entry.records_success}"
            if entry.records_error > 0:
                msg += f" | Errors: {entry.records_error}"
        return msg
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"{ETLConstants.PREFIX.LOG_ID}{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries"""
        return [entry.to_dict() for entry in self.log_entries]
    
    def get_summary(self) -> dict:
        """Get logging summary statistics"""
        total_entries = len(self.log_entries)
        errors = sum(1 for e in self.log_entries if e.status == ETLConstants.STATUS.ERROR)
        warnings = sum(1 for e in self.log_entries if e.status == ETLConstants.STATUS.WARNING)
        success = sum(1 for e in self.log_entries if e.status == ETLConstants.STATUS.SUCCESS)
        
        return {
            'etl_run_id': self.etl_run_id,
            'total_entries': total_entries,
            'errors': errors,
            'warnings': warnings,
            'success': success
        }