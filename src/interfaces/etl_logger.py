"""
Module: etl_logger
Description: Abstract Base Class for ETL Logger Interface
Converted from: ZIF_ETL_LOGGER ABAP interface
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional


class LogStatus(str, Enum):
    """Log status enumeration."""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(str, Enum):
    """ETL process step enumeration."""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class ETLLoggerInterface(ABC):
    """
    Abstract Base Class for ETL logging.
    Converted from: ZIF_ETL_LOGGER ABAP interface
    
    Provides standardized logging interface for all ETL components.
    """

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
        Log a message with ETL execution details.
        
        Args:
            step: Process step identifier
            status: Log status (success, error, warning, info)
            message: Log message text
            records_processed: Total number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
        """
        pass

    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the unique ETL run identifier.
        
        Returns:
            str: ETL run ID
        """
        pass