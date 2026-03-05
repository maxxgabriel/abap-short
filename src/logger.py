"""
Logger Module - Sales ETL System
Utility class for ETL logging
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import logging


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


class ETLLogger:
    """Utility class for ETL logging"""
    
    def __init__(self, etl_run_id: str):
        """
        Initialize logger
        
        Args:
            etl_run_id: Unique ETL run identifier
        """
        self.etl_run_id = etl_run_id
        self.log = logging.getLogger(__name__)
        self.log_entries = []
    
    def log_message(
        self,
        step: str,
        status: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        message: str = ""
    ) -> None:
        """
        Log ETL message
        
        Args:
            step: Process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            message: Log message
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
        
        # Console output
        log_level = self._get_log_level(status)
        log_msg = (
            f"{log_entry.execution_time} | {log_entry.process_step:12} | "
            f"{log_entry.status} | {log_entry.message}"
        )
        
        if records_processed > 0:
            log_msg += f" [Processed: {records_processed}, Success: {records_success}, Error: {records_error}]"
        
        self.log.log(log_level, log_msg)
        
        # In production, insert to database
        # INSERT INTO zetl_log VALUES (...)
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries"""
        return self.log_entries
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to logging level"""
        level_map = {
            "S": logging.INFO,
            "I": logging.INFO,
            "W": logging.WARNING,
            "E": logging.ERROR
        }
        return level_map.get(status, logging.INFO)