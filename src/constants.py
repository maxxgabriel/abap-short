"""
ETL Constants Module
Defines business rules, thresholds, and configuration constants.
"""

from enum import Enum
from decimal import Decimal


class Status(Enum):
    """Status codes for ETL processing."""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process step identifiers."""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class Category(Enum):
    """Sale category classifications."""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


class BusinessRules:
    """Business rules and thresholds for ETL processing."""
    
    # Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Decimal = Decimal('0.10')
    
    # Tax rate
    TAX_RATE: Decimal = Decimal('0.08')
    
    # Cost ratio for profit calculation
    COST_RATIO: Decimal = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal('500.00')


class ETLConfig:
    """ETL processing configuration defaults."""
    
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600


class IDPrefixes:
    """ID generation prefixes."""
    
    ETL_RUN = 'ETL'
    LOG_ID = 'LOG'
    ANALYTICS_ID = 'ANL'


class Messages:
    """Standard message templates."""
    
    INIT_SUCCESS = 'ETL process initialized successfully'
    EXTRACT_START = 'Starting data extraction'
    EXTRACT_COMPLETE = 'Data extraction completed'
    TRANSFORM_START = 'Starting data transformation'
    TRANSFORM_COMPLETE = 'Data transformation completed'
    LOAD_START = 'Starting data load'
    LOAD_COMPLETE = 'Data load completed'
    ETL_COMPLETE = 'ETL process completed successfully'
    ETL_ERROR = 'ETL process failed'