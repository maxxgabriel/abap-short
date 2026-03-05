"""
ETL Logger Interface - Abstract Base Class
Defines the contract for ETL logging implementations with integration to Python logging framework.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional
from datetime import datetime


class LogStatus(Enum):
    """Log status codes for ETL operations"""
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


class ETLLoggerInterface(ABC):
    """
    Abstract base class for ETL logging operations.
    
    This interface defines the contract that all ETL logger implementations
    must follow. It integrates with Python's logging framework and provides
    structured logging for ETL operations.
    """
    
    @abstractmethod
    def __init__(self, etl_run_id: str) -> None:
        """
        Initialize the logger with an ETL run ID.
        
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
        records_error: int = 0,
        exception: Optional[Exception] = None
    ) -> None:
        """
        Log a message for an ETL process step.
        
        Args:
            step: The ETL process step (from ProcessStep enum)
            status: The log status (from LogStatus enum)
            message: The log message text
            records_processed: Total number of records processed (default: 0)
            records_success: Number of successfully processed records (default: 0)
            records_error: Number of records with errors (default: 0)
            exception: Optional exception object for error logging
        """
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: The ETL run ID
        """
        pass
    
    @abstractmethod
    def get_log_entries(self) -> list:
        """
        Retrieve all log entries for the current ETL run.
        
        Returns:
            list: List of log entry dictionaries
        """
        pass
    
    @abstractmethod
    def flush(self) -> None:
        """
        Flush any buffered log entries to the target storage.
        """
        pass


class LogEntry:
    """
    Structured log entry data class.
    """
    
    def __init__(
        self,
        log_id: str,
        etl_run_id: str,
        execution_date: datetime,
        execution_time: datetime,
        process_step: ProcessStep,
        status: LogStatus,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        message: str = ""
    ):
        """
        Initialize a log entry.
        
        Args:
            log_id: Unique log entry identifier
            etl_run_id: ETL run identifier
            execution_date: Date of execution
            execution_time: Time of execution
            process_step: Process step enum
            status: Log status enum
            records_processed: Total records processed
            records_success: Successful records
            records_error: Error records
            message: Log message
        """
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