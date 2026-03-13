"""
Constants and Enumerations for ETL System

Migrated from ABAP ZCL_ETL_CONSTANTS class.
Provides status codes, process steps, categories, and ID prefixes.
"""

from enum import Enum
from typing import Final


class StatusCode(str, Enum):
    """ETL process status codes"""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ProcessStep(str, Enum):
    """ETL process steps"""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategory(str, Enum):
    """Sale categorization levels"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class IDPrefix(str, Enum):
    """ID prefixes for generated identifiers"""
    ETL_RUN = "ETL"
    LOG = "LOG"
    ANALYTICS = "ANL"


class MessageText:
    """Standard message texts for logging"""
    INIT_SUCCESS: Final[str] = "ETL process initialized successfully"
    EXTRACT_START: Final[str] = "Starting data extraction"
    EXTRACT_COMPLETE: Final[str] = "Data extraction completed"
    TRANSFORM_START: Final[str] = "Starting data transformation"
    TRANSFORM_COMPLETE: Final[str] = "Data transformation completed"
    LOAD_START: Final[str] = "Starting data load"
    LOAD_COMPLETE: Final[str] = "Data load completed"
    ETL_COMPLETE: Final[str] = "ETL process completed successfully"
    ETL_ERROR: Final[str] = "ETL process failed"


class ETLDefaults:
    """Default configuration values for ETL process"""
    BATCH_SIZE: Final[int] = 1000
    COMMIT_INTERVAL: Final[int] = 500
    RETRY_ATTEMPTS: Final[int] = 3
    TIMEOUT_SECONDS: Final[int] = 3600