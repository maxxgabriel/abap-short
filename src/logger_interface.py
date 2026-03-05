"""
ETL Logger Interface - Python Abstract Base Class
Converted from ABAP interface ZIF_ETL_LOGGER
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional


class LogStatus(Enum):
    """
    ETL log status codes.
    Converted from ZIF_ETL_LOGGER=>gc_status
    """
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(Enum):
    """
    ETL process step identifiers.
    Converted from ZIF_ETL_LOGGER=>gc_step
    """
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class ETLLoggerInterface(ABC):
    """
    Abstract base class for ETL logging.
    Defines the contract for all ETL logger implementations.
    
    Converted from ABAP interface: ZIF_ETL_LOGGER
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
        Log an ETL process message.
        
        Args:
            step: The ETL process step (e.g., EXTRACT, TRANSFORM)
            status: Status code (SUCCESS, ERROR, WARNING, INFO)
            message: Log message text (max 255 chars)
            records_processed: Total number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
        """
        pass

    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            Unique ETL run ID (20 character string)
        """
        pass