"""
ETL Constants and Configuration
Centralized constants for the ETL system
"""
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ETLConstants:
    """Immutable constants for ETL processing"""
    
    # Status codes
    STATUS_NEW: str = 'N'
    STATUS_PROCESSED: str = 'P'
    STATUS_ERROR: str = 'E'
    STATUS_WARNING: str = 'W'
    STATUS_SUCCESS: str = 'S'
    STATUS_INFO: str = 'I'
    
    # Process steps
    STEP_INIT: str = 'INIT'
    STEP_EXTRACT: str = 'EXTRACT'
    STEP_TRANSFORM: str = 'TRANSFORM'
    STEP_LOAD: str = 'LOAD'
    STEP_VALIDATE: str = 'VALIDATE'
    STEP_COMPLETE: str = 'COMPLETE'
    STEP_ERROR: str = 'ERROR'
    
    # Sale categories
    CATEGORY_HIGH: str = 'HIGH'
    CATEGORY_MEDIUM: str = 'MEDIUM'
    CATEGORY_LOW: str = 'LOW'
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Decimal = Decimal('0.10')
    
    # Business rules - Tax rate
    TAX_RATE: Decimal = Decimal('0.08')
    
    # Business rules - Cost ratio
    COST_RATIO: Decimal = Decimal('0.60')
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal('500.00')
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN: str = 'ETL'
    PREFIX_LOG_ID: str = 'LOG'
    PREFIX_ANALYTICS_ID: str = 'ANL'
    
    # Message texts
    MSG_INIT_SUCCESS: str = 'ETL process initialized successfully'
    MSG_EXTRACT_START: str = 'Starting data extraction'
    MSG_EXTRACT_COMPLETE: str = 'Data extraction completed'
    MSG_TRANSFORM_START: str = 'Starting data transformation'
    MSG_TRANSFORM_COMPLETE: str = 'Data transformation completed'
    MSG_LOAD_START: str = 'Starting data load'
    MSG_LOAD_COMPLETE: str = 'Data load completed'
    MSG_ETL_COMPLETE: str = 'ETL process completed successfully'
    MSG_ETL_ERROR: str = 'ETL process failed'


# Global constants instance
CONSTANTS = ETLConstants()