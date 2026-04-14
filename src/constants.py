"""
Constants and configuration enums for ETL system.
Migrated from ABAP ZCL_ETL_CONSTANTS class.
"""
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Final


class StatusCode(str, Enum):
    """ETL process status codes."""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ProcessStep(str, Enum):
    """ETL process step identifiers."""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategory(str, Enum):
    """Sale categorization levels."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True)
class BusinessRules:
    """Business rules for ETL transformations."""
    
    # Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal("0.05")
    DISCOUNT_RATE_TIER2: Decimal = Decimal("0.10")
    
    # Tax configuration
    TAX_RATE: Decimal = Decimal("0.08")
    
    # Cost calculation
    COST_RATIO: Decimal = Decimal("0.60")
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal("500.00")


@dataclass(frozen=True)
class ETLConfig:
    """ETL process configuration defaults."""
    
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600


@dataclass(frozen=True)
class IDPrefixes:
    """Prefix constants for ID generation."""
    
    ETL_RUN: str = "ETL"
    LOG_ID: str = "LOG"
    ANALYTICS_ID: str = "ANL"


@dataclass(frozen=True)
class Messages:
    """Standard ETL process messages."""
    
    INIT_SUCCESS: str = "ETL process initialized successfully"
    EXTRACT_START: str = "Starting data extraction"
    EXTRACT_COMPLETE: str = "Data extraction completed"
    TRANSFORM_START: str = "Starting data transformation"
    TRANSFORM_COMPLETE: str = "Data transformation completed"
    LOAD_START: str = "Starting data load"
    LOAD_COMPLETE: str = "Data load completed"
    ETL_COMPLETE: str = "ETL process completed successfully"
    ETL_ERROR: str = "ETL process failed"


# Singleton instances for easy access
BUSINESS_RULES: Final[BusinessRules] = BusinessRules()
ETL_CONFIG: Final[ETLConfig] = ETLConfig()
ID_PREFIXES: Final[IDPrefixes] = IDPrefixes()
MESSAGES: Final[Messages] = Messages()