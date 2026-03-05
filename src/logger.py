"""
Concrete ETL Logger Implementation
Implements the ETL logger interface with Python logging framework integration.
"""

import logging
from datetime import datetime
from typing import List, Optional
from src.logger_interface import (
    ETLLoggerInterface,
    ETLLogEntry,
    ETLStatus,
    ETLStep
)


class ETLLogger(ETLLoggerInterface):
    """
    Concrete implementation of ETL logger using Python's logging framework.
    Maintains ETL-specific log entries while integrating with standard logging.
    """

    def __init__(self, etl_run_id: str, logger_name: str = 'etl_logger'):
        """
        Initialize the ETL logger.

        Args:
            etl_run_id: Unique identifier for this ETL run
            logger_name: Name for the Python logger instance
        """
        self._etl_run_id = etl_run_id
        self._log_entries: List[ETLLogEntry] = []
        self._logger = logging.getLogger(logger_name)
        self._log_counter = 0

        # Configure logger if not already configured
        if not self._logger.handlers:
            self._configure_logger()

    def _configure_logger(self) -> None:
        """Configure the Python logging framework."""
        self._logger.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # File handler
        file_handler = logging.FileHandler(
            f'etl_run_{self._etl_run_id}.log'
        )
        file_handler.setLevel(logging.DEBUG)

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        self._logger.addHandler(console_handler)
        self._logger.addHandler(file_handler)

    def _generate_log_id(self) -> str:
        """Generate a unique log entry ID."""
        self._log_counter += 1
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG{timestamp}{self._log_counter:04d}"

    def _map_status_to_log_level(self, status: ETLStatus) -> int:
        """Map ETL status to Python logging level."""
        mapping = {
            ETLStatus.SUCCESS: logging.INFO,
            ETLStatus.INFO: logging.INFO,
            ETLStatus.WARNING: logging.WARNING,
            ETLStatus.ERROR: logging.ERROR
        }
        return mapping.get(status, logging.INFO)

    def log_message(
        self,
        step: ETLStep,
        status: ETLStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log an ETL process message.

        Args:
            step: The ETL process step
            status: The status of the operation
            message: The log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()

        # Create log entry
        log_entry = ETLLogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_date=now,
            execution_time=now,
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )

        # Store log entry
        self._log_entries.append(log_entry)

        # Log to Python logging framework
        log_level = self._map_status_to_log_level(status)
        formatted_message = (
            f"[{step.value}] [{status.value}] {message}"
        )
        if records_processed > 0:
            formatted_message += (
                f" | Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Error: {records_error}"
            )

        self._logger.log(log_level, formatted_message)

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.

        Returns:
            The ETL run ID string
        """
        return self._etl_run_id

    def set_etl_run_id(self, run_id: str) -> None:
        """
        Set the ETL run identifier.

        Args:
            run_id: The ETL run ID to set
        """
        self._etl_run_id = run_id
        self._logger.info(f"ETL Run ID updated to: {run_id}")

    def get_log_entries(self) -> List[dict]:
        """
        Retrieve all log entries for the current ETL run.

        Returns:
            List of log entry dictionaries
        """
        return [entry.to_dict() for entry in self._log_entries]

    def clear_logs(self) -> None:
        """Clear all log entries for the current run."""
        self._log_entries.clear()
        self._log_counter = 0
        self._logger.info(f"Cleared logs for ETL run: {self._etl_run_id}")

    def get_statistics(self) -> dict:
        """
        Get logging statistics for the current run.

        Returns:
            Dictionary with logging statistics
        """
        total_entries = len(self._log_entries)
        status_counts = {
            'success': sum(1 for e in self._log_entries 
                          if e.status == ETLStatus.SUCCESS),
            'error': sum(1 for e in self._log_entries 
                        if e.status == ETLStatus.ERROR),
            'warning': sum(1 for e in self._log_entries 
                          if e.status == ETLStatus.WARNING),
            'info': sum(1 for e in self._log_entries 
                       if e.status == ETLStatus.INFO)
        }

        return {
            'total_entries': total_entries,
            'status_counts': status_counts,
            'etl_run_id': self._etl_run_id
        }