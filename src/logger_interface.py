"""
ETL Logger Interface and Protocol Definition
Converts ABAP ZIF_ETL_LOGGER interface to Python ABC with protocol definition
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Protocol, runtime_checkable


class LogStatus(Enum):
    """ETL logging status codes"""
    SUCCESS = "S"
    ERROR = "E"
    WARNING = "W"
    INFO = "I"


class ProcessStep(Enum):
    """ETL process steps"""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


@runtime_checkable
class ETLLoggerProtocol(Protocol):
    """
    Protocol definition for ETL logger implementations.
    This defines the contract that all logger implementations must follow.
    """
    
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
        Log an ETL process message
        
        Args:
            step: The process step being logged
            status: The status of the operation
            message: Descriptive message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Failed records
        """
        ...
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier
        
        Returns:
            ETL run ID string
        """
        ...


class IETLLogger(ABC):
    """
    Abstract base class for ETL logging.
    Converted from ABAP interface ZIF_ETL_LOGGER.
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
        Log an ETL process message
        
        Args:
            step: The process step being logged
            status: The status of the operation
            message: Descriptive message
            records_processed: Total records processed (default: 0)
            records_success: Successfully processed records (default: 0)
            records_error: Failed records (default: 0)
        """
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier
        
        Returns:
            ETL run ID string
        """
        pass


class ETLComponentStatus(Enum):
    """Component execution status codes"""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class SaleCategory(Enum):
    """Sales categorization levels"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ETLConstants:
    """
    ETL configuration constants.
    Converted from ABAP class ZCL_ETL_CONSTANTS.
    """
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = 0.05
    DISCOUNT_RATE_TIER2 = 0.10
    
    # Business rules - Tax rate
    TAX_RATE = 0.08
    
    # Business rules - Cost ratio
    COST_RATIO = 0.60
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = 2000.00
    CATEGORY_MEDIUM_THRESHOLD = 500.00
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN = "ETL"
    PREFIX_LOG_ID = "LOG"
    PREFIX_ANALYTICS_ID = "ANL"
    
    # Message texts
    MSG_INIT_SUCCESS = "ETL process initialized successfully"
    MSG_EXTRACT_START = "Starting data extraction"
    MSG_EXTRACT_COMPLETE = "Data extraction completed"
    MSG_TRANSFORM_START = "Starting data transformation"
    MSG_TRANSFORM_COMPLETE = "Data transformation completed"
    MSG_LOAD_START = "Starting data load"
    MSG_LOAD_COMPLETE = "Data load completed"
    MSG_ETL_COMPLETE = "ETL process completed successfully"
    MSG_ETL_ERROR = "ETL process failed"