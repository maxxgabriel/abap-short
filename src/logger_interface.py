"""
ETL Logger Interface Module
Defines the abstract base class for ETL logging integrated with Python's logging framework.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional
from datetime import datetime


class ETLStatus(Enum):
    """Enumeration for ETL process statuses."""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ETLStep(Enum):
    """Enumeration for ETL process steps."""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class ETLLoggerInterface(ABC):
    """
    Abstract base class for ETL logging functionality.
    Integrates with Python's logging framework while maintaining
    ETL-specific logging requirements.
    """

    @abstractmethod
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
        pass

    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.

        Returns:
            The ETL run ID string
        """
        pass

    @abstractmethod
    def set_etl_run_id(self, run_id: str) -> None:
        """
        Set the ETL run identifier.

        Args:
            run_id: The ETL run ID to set
        """
        pass

    @abstractmethod
    def get_log_entries(self) -> list:
        """
        Retrieve all log entries for the current ETL run.

        Returns:
            List of log entry dictionaries
        """
        pass

    @abstractmethod
    def clear_logs(self) -> None:
        """Clear all log entries for the current run."""
        pass


class ETLLogEntry:
    """Data class representing a single ETL log entry."""

    def __init__(
        self,
        log_id: str,
        etl_run_id: str,
        execution_date: datetime,
        execution_time: datetime,
        process_step: ETLStep,
        status: ETLStatus,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        message: str = ""
    ):
        self.log_id = log_id
        self.etl_run_id = etl_run_id
        self.execution_date = execution_date
        self.execution_time = execution_time
        self.process_step = process_step
        self.status = status
        self.records_processed = records_processed
        self.records_success = records_success
        self.records_error = records_error
        self.message = message

    def to_dict(self) -> dict:
        """Convert log entry to dictionary format."""
        return {
            'log_id': self.log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': self.execution_date.strftime('%Y-%m-%d'),
            'execution_time': self.execution_time.strftime('%H:%M:%S'),
            'process_step': self.process_step.value,
            'status': self.status.value,
            'records_processed': self.records_processed,
            'records_success': self.records_success,
            'records_error': self.records_error,
            'message': self.message
        }

    def __repr__(self) -> str:
        return (
            f"ETLLogEntry(log_id='{self.log_id}', "
            f"step={self.process_step.name}, "
            f"status={self.status.name}, "
            f"message='{self.message}')"
        )