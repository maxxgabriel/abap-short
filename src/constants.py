"""
Constants and configuration for ETL system.
Converted from ABAP ZCL_ETL_CONSTANTS class.
"""
from decimal import Decimal
from typing import Final


class ETLConstants:
    """Container for all ETL constants."""
    
    # Status codes (from gc_status)
    class Status:
        NEW: Final[str] = "N"
        PROCESSED: Final[str] = "P"
        ERROR: Final[str] = "E"
        WARNING: Final[str] = "W"
        SUCCESS: Final[str] = "S"
        INFO: Final[str] = "I"
    
    # ETL process steps (from gc_step)
    class Step:
        INIT: Final[str] = "INIT"
        EXTRACT: Final[str] = "EXTRACT"
        TRANSFORM: Final[str] = "TRANSFORM"
        LOAD: Final[str] = "LOAD"
        VALIDATE: Final[str] = "VALIDATE"
        COMPLETE: Final[str] = "COMPLETE"
        ERROR: Final[str] = "ERROR"
    
    # Sale categories (from gc_category)
    class Category:
        HIGH: Final[str] = "HIGH"
        MEDIUM: Final[str] = "MEDIUM"
        LOW: Final[str] = "LOW"
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1: Final[int] = 10
    DISCOUNT_QTY_TIER2: Final[int] = 15
    DISCOUNT_RATE_TIER1: Final[Decimal] = Decimal("0.05")
    DISCOUNT_RATE_TIER2: Final[Decimal] = Decimal("0.10")
    
    # Business rules - Tax rate
    TAX_RATE: Final[Decimal] = Decimal("0.08")
    
    # Business rules - Cost ratio
    COST_RATIO: Final[Decimal] = Decimal("0.60")
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD: Final[Decimal] = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD: Final[Decimal] = Decimal("500.00")
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE: Final[int] = 1000
    DEFAULT_COMMIT_INTERVAL: Final[int] = 500
    DEFAULT_RETRY_ATTEMPTS: Final[int] = 3
    DEFAULT_TIMEOUT_SECONDS: Final[int] = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN: Final[str] = "ETL"
    PREFIX_LOG_ID: Final[str] = "LOG"
    PREFIX_ANALYTICS_ID: Final[str] = "ANL"
    
    # Message texts
    class Messages:
        INIT_SUCCESS: Final[str] = "ETL process initialized successfully"
        EXTRACT_START: Final[str] = "Starting data extraction"
        EXTRACT_COMPLETE: Final[str] = "Data extraction completed"
        TRANSFORM_START: Final[str] = "Starting data transformation"
        TRANSFORM_COMPLETE: Final[str] = "Data transformation completed"
        LOAD_START: Final[str] = "Starting data load"
        LOAD_COMPLETE: Final[str] = "Data load completed"
        ETL_COMPLETE: Final[str] = "ETL process completed successfully"
        ETL_ERROR: Final[str] = "ETL process failed"