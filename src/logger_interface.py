"""
ETL Logger Interface - Abstract Base Class
Provides standardized logging interface for ETL components with Python logging integration.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional
from datetime import datetime


class LogStatus(Enum):
    """ETL logging status codes"""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process step identifiers"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class IETLLogger(ABC):
    """
    Abstract base class for ETL logging.
    Defines the contract for all ETL logger implementations.
    """

    @abstractmethod
    def __init__(self, etl_run_id: str):
        """
        Initialize logger with ETL run identifier.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
        """
        pass

    @abstractmethod
    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message for an ETL process step.
        
        Args:
            step: The ETL process step
            status: Log status level
            message: Log message content
            records_processed: Total number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
        """
        pass

    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run identifier.
        
        Returns:
            The ETL run ID string
        """
        pass

    @abstractmethod
    def get_log_entries(self) -> list:
        """
        Retrieve all log entries for this ETL run.
        
        Returns:
            List of log entry dictionaries
        """
        pass


class LogEntry:
    """Data class representing a single log entry"""
    
    def __init__(
        self,
        log_id: str,
        etl_run_id: str,
        execution_datetime: datetime,
        process_step: ProcessStep,
        status: LogStatus,
        records_processed: int,
        records_success: int,
        records_error: int,
        message: str
    ):
        self.log_id = log_id
        self.etl_run_id = etl_run_id
        self.execution_datetime = execution_datetime
        self.process_step = process_step
        self.status = status
        self.records_processed = records_processed
        self.records_success = records_success
        self.records_error = records_error
        self.message = message

    def to_dict(self) -> dict:
        """Convert log entry to dictionary format"""
        return {
            'log_id': self.log_id,
            'etl_run_id': self.etl_run_id,
            'execution_datetime': self.execution_datetime.isoformat(),
            'execution_date': self.execution_datetime.date().isoformat(),
            'execution_time': self.execution_datetime.time().isoformat(),
            'process_step': self.process_step.value,
            'status': self.status.value,
            'records_processed': self.records_processed,
            'records_success': self.records_success,
            'records_error': self.records_error,
            'message': self.message
        }