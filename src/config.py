"""
Configuration module for Sales ETL System.
Provides type-safe enums and Decimal types for numeric precision.
Migrated from ABAP ZCL_ETL_CONSTANTS class.
"""

from decimal import Decimal
from enum import Enum
from typing import Final


class StatusCode(Enum):
    """Status codes for ETL processing."""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process steps."""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class SaleCategory(Enum):
    """Sale categorization levels."""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


class BusinessRules:
    """Business rules for sales calculations with Decimal precision."""
    
    # Discount thresholds and rates
    DISCOUNT_QTY_TIER1: Final[int] = 10
    DISCOUNT_QTY_TIER2: Final[int] = 15
    DISCOUNT_RATE_TIER1: Final[Decimal] = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Final[Decimal] = Decimal('0.10')
    
    # Tax rate
    TAX_RATE: Final[Decimal] = Decimal('0.08')
    
    # Cost ratio for profit margin calculation
    COST_RATIO: Final[Decimal] = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Final[Decimal] = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Final[Decimal] = Decimal('500.00')


class ETLConfig:
    """ETL configuration defaults."""
    
    DEFAULT_BATCH_SIZE: Final[int] = 1000
    DEFAULT_COMMIT_INTERVAL: Final[int] = 500
    DEFAULT_RETRY_ATTEMPTS: Final[int] = 3
    DEFAULT_TIMEOUT_SECONDS: Final[int] = 3600


class IDPrefix:
    """ID prefixes for generated identifiers."""
    
    ETL_RUN: Final[str] = 'ETL'
    LOG_ID: Final[str] = 'LOG'
    ANALYTICS_ID: Final[str] = 'ANL'


class Messages:
    """Standard message texts for ETL process."""
    
    INIT_SUCCESS: Final[str] = 'ETL process initialized successfully'
    EXTRACT_START: Final[str] = 'Starting data extraction'
    EXTRACT_COMPLETE: Final[str] = 'Data extraction completed'
    TRANSFORM_START: Final[str] = 'Starting data transformation'
    TRANSFORM_COMPLETE: Final[str] = 'Data transformation completed'
    LOAD_START: Final[str] = 'Starting data load'
    LOAD_COMPLETE: Final[str] = 'Data load completed'
    ETL_COMPLETE: Final[str] = 'ETL process completed successfully'
    ETL_ERROR: Final[str] = 'ETL process failed'


class ETLConstants:
    """
    Main constants class aggregating all configuration.
    Provides a single access point for all ETL constants.
    """
    
    status = StatusCode
    step = ProcessStep
    category = SaleCategory
    business_rules = BusinessRules
    config = ETLConfig
    prefix = IDPrefix
    messages = Messages