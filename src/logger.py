"""
ETL logging utility.

This module provides logging functionality for the ETL system,
replacing the ABAP ZCL_ETL_LOGGER class.
"""

import logging
from datetime import datetime
from typing import Optional

from src.constants import ProcessStep, Status
from src.models import ETLLogEntry


class ETLLogger:
    """
    Logger for ETL operations.
    
    Provides structured logging with ETL-specific context and metadata.
    """
    
    def __init__(self, etl_run_id: str, log_level: int = logging.INFO):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            log_level: Python logging level
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Configure Python logger
        self._logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._logger.setLevel(log_level)
        
        # Add handler if not already present
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
    
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
            status: Status code
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        # Create log entry
        log_entry = ETLLogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.date(),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_at=now
        )
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_message = (
            f"[{step}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Error: {records_error})"
        )
        
        if status == Status.ERROR:
            self._logger.error(log_message)
        elif status == Status.WARNING:
            self._logger.warning(log_message)
        elif status == Status.INFO:
            self._logger.info(log_message)
        else:
            self._logger.info(log_message)
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            str: ETL run ID
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list[ETLLogEntry]:
        """
        Get all log entries for this ETL run.
        
        Returns:
            list[ETLLogEntry]: List of log entries
        """
        return self.log_entries.copy()
    
    @staticmethod
    def _generate_log_id() -> str:
        """
        Generate a unique log ID.
        
        Returns:
            str: Unique log ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"