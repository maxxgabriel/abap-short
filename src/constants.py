"""
ETL Constants Module
Provides configuration constants and business rules for the ETL pipeline.
"""
from decimal import Decimal
from typing import Final


class ETLStatus:
    """Status codes for ETL processing."""
    NEW: Final[str] = 'N'
    PROCESSED: Final[str] = 'P'
    ERROR: Final[str] = 'E'
    WARNING: Final[str] = 'W'
    SUCCESS: Final[str] = 'S'
    INFO: Final[str] = 'I'


class ETLStep:
    """ETL process step identifiers."""
    INIT: Final[str] = 'INIT'
    EXTRACT: Final[str] = 'EXTRACT'
    TRANSFORM: Final[str] = 'TRANSFORM'
    LOAD: Final[str] = 'LOAD'
    VALIDATE: Final[str] = 'VALIDATE'
    COMPLETE: Final[str] = 'COMPLETE'
    ERROR: Final[str] = 'ERROR'


class SaleCategory:
    """Sale categorization values."""
    HIGH: Final[str] = 'HIGH'
    MEDIUM: Final[str] = 'MEDIUM'
    LOW: Final[str] = 'LOW'


class BusinessRules:
    """Business rules for ETL transformations."""
    # Discount thresholds
    DISCOUNT_QTY_TIER1: Final[int] = 10
    DISCOUNT_QTY_TIER2: Final[int] = 15
    DISCOUNT_RATE_TIER1: Final[Decimal] = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Final[Decimal] = Decimal('0.10')
    
    # Tax and cost
    TAX_RATE: Final[Decimal] = Decimal('0.08')
    COST_RATIO: Final[Decimal] = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Final[Decimal] = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Final[Decimal] = Decimal('500.00')


class ETLConfig:
    """Default ETL configuration values."""
    DEFAULT_BATCH_SIZE: Final[int] = 1000
    DEFAULT_COMMIT_INTERVAL: Final[int] = 500
    DEFAULT_RETRY_ATTEMPTS: Final[int] = 3
    DEFAULT_TIMEOUT_SECONDS: Final[int] = 3600


class IDPrefixes:
    """Prefixes for generated IDs."""
    ETL_RUN: Final[str] = 'ETL'
    LOG_ID: Final[str] = 'LOG'
    ANALYTICS_ID: Final[str] = 'ANL'


class Messages:
    """Standard message texts."""
    INIT_SUCCESS: Final[str] = 'ETL process initialized successfully'
    EXTRACT_START: Final[str] = 'Starting data extraction'
    EXTRACT_COMPLETE: Final[str] = 'Data extraction completed'
    TRANSFORM_START: Final[str] = 'Starting data transformation'
    TRANSFORM_COMPLETE: Final[str] = 'Data transformation completed'
    LOAD_START: Final[str] = 'Starting data load'
    LOAD_COMPLETE: Final[str] = 'Data load completed'
    ETL_COMPLETE: Final[str] = 'ETL process completed successfully'
    ETL_ERROR: Final[str] = 'ETL process failed'