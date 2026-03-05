"""
ETL system constants and configuration values.

This module centralizes all constant values used throughout the ETL system,
replacing the ABAP ZCL_ETL_CONSTANTS class.
"""

from decimal import Decimal


class Status:
    """Status codes for ETL processing."""
    
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ProcessStep:
    """ETL process steps."""
    
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategory:
    """Sale categories based on amount."""
    
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class BusinessRules:
    """Business rules and thresholds."""
    
    # Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal("0.05")
    DISCOUNT_RATE_TIER2 = Decimal("0.10")
    
    # Tax rate
    TAX_RATE = Decimal("0.08")
    
    # Cost ratio
    COST_RATIO = Decimal("0.60")
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD = Decimal("500.00")


class ETLDefaults:
    """Default ETL configuration values."""
    
    BATCH_SIZE = 1000
    COMMIT_INTERVAL = 500
    RETRY_ATTEMPTS = 3
    TIMEOUT_SECONDS = 3600


class IDPrefixes:
    """Prefixes for generated IDs."""
    
    ETL_RUN = "ETL"
    LOG_ID = "LOG"
    ANALYTICS_ID = "ANL"


class Messages:
    """Standard message templates."""
    
    INIT_SUCCESS = "ETL process initialized successfully"
    EXTRACT_START = "Starting data extraction"
    EXTRACT_COMPLETE = "Data extraction completed"
    TRANSFORM_START = "Starting data transformation"
    TRANSFORM_COMPLETE = "Data transformation completed"
    LOAD_START = "Starting data load"
    LOAD_COMPLETE = "Data load completed"
    ETL_COMPLETE = "ETL process completed successfully"
    ETL_ERROR = "ETL process failed"