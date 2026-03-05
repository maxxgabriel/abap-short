"""
ETL Logger Abstract Base Class Module

Provides abstract base class for ETL logging with integration to Python logging framework
and Enum-based constants for status and process steps.
"""

from abc import ABC, abstractmethod
from enum import Enum
from datetime import datetime
from typing import Optional
from dataclasses import dataclass


class LogStatus(Enum):
    """Enumeration for log status codes"""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(Enum):
    """Enumeration for ETL process steps"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


@dataclass
class LogEntry:
    """Data class representing a log entry"""
    log_id: str
    etl_run_id: str
    execution_date: datetime
    process_step: ProcessStep
    status: LogStatus
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""


class ETLLoggerInterface(ABC):
    """
    Abstract Base Class for ETL Logger
    
    Defines the contract for ETL logging implementations with integration
    to Python's standard logging framework and Enum-based constants.
    """
    
    @abstractmethod
    def __init__(self, etl_run_id: str):
        """
        Initialize the logger with ETL run ID
        
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
        Log a message with ETL context
        
        Args:
            step: ETL process step
            status: Log status
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
        """
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID
        
        Returns:
            ETL run ID
        """
        pass
    
    @abstractmethod
    def get_log_entries(self) -> list[LogEntry]:
        """
        Get all log entries for this ETL run
        
        Returns:
            List of log entries
        """
        pass
    
    @abstractmethod
    def generate_log_id(self) -> str:
        """
        Generate a unique log entry ID
        
        Returns:
            Unique log ID
        """
        pass