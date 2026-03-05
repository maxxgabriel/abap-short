"""
ETL Constants and Configuration Values
Migrated from ZCL_ETL_CONSTANTS
"""

from typing import Dict, Any
from decimal import Decimal


class ETLConstants:
    """Central constants and configuration for ETL system"""
    
    # Status codes
    class Status:
        NEW = "N"
        PROCESSED = "P"
        ERROR = "E"
        WARNING = "W"
        SUCCESS = "S"
        INFO = "I"
    
    # Process steps
    class Step:
        INIT = "INIT"
        EXTRACT = "EXTRACT"
        TRANSFORM = "TRANSFORM"
        LOAD = "LOAD"
        VALIDATE = "VALIDATE"
        COMPLETE = "COMPLETE"
        ERROR = "ERROR"
    
    # Sale categories
    class Category:
        HIGH = "HIGH"
        MEDIUM = "MEDIUM"
        LOW = "LOW"
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal("0.05")
    DISCOUNT_RATE_TIER2 = Decimal("0.10")
    
    # Business rules - Tax rate
    TAX_RATE = Decimal("0.08")
    
    # Business rules - Cost ratio
    COST_RATIO = Decimal("0.60")
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD = Decimal("500.00")
    
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
    class Messages:
        INIT_SUCCESS = "ETL process initialized successfully"
        EXTRACT_START = "Starting data extraction"
        EXTRACT_COMPLETE = "Data extraction completed"
        TRANSFORM_START = "Starting data transformation"
        TRANSFORM_COMPLETE = "Data transformation completed"
        LOAD_START = "Starting data load"
        LOAD_COMPLETE = "Data load completed"
        ETL_COMPLETE = "ETL process completed successfully"
        ETL_ERROR = "ETL process failed"
    
    @classmethod
    def get_all_constants(cls) -> Dict[str, Any]:
        """Get all constants as a dictionary"""
        return {
            "status": {
                "new": cls.Status.NEW,
                "processed": cls.Status.PROCESSED,
                "error": cls.Status.ERROR,
                "warning": cls.Status.WARNING,
                "success": cls.Status.SUCCESS,
                "info": cls.Status.INFO,
            },
            "steps": {
                "init": cls.Step.INIT,
                "extract": cls.Step.EXTRACT,
                "transform": cls.Step.TRANSFORM,
                "load": cls.Step.LOAD,
                "validate": cls.Step.VALIDATE,
                "complete": cls.Step.COMPLETE,
                "error": cls.Step.ERROR,
            },
            "categories": {
                "high": cls.Category.HIGH,
                "medium": cls.Category.MEDIUM,
                "low": cls.Category.LOW,
            },
            "business_rules": {
                "discount_qty_tier1": cls.DISCOUNT_QTY_TIER1,
                "discount_qty_tier2": cls.DISCOUNT_QTY_TIER2,
                "discount_rate_tier1": float(cls.DISCOUNT_RATE_TIER1),
                "discount_rate_tier2": float(cls.DISCOUNT_RATE_TIER2),
                "tax_rate": float(cls.TAX_RATE),
                "cost_ratio": float(cls.COST_RATIO),
                "category_high_threshold": float(cls.CATEGORY_HIGH_THRESHOLD),
                "category_medium_threshold": float(cls.CATEGORY_MEDIUM_THRESHOLD),
            },
            "defaults": {
                "batch_size": cls.DEFAULT_BATCH_SIZE,
                "commit_interval": cls.DEFAULT_COMMIT_INTERVAL,
                "retry_attempts": cls.DEFAULT_RETRY_ATTEMPTS,
                "timeout_seconds": cls.DEFAULT_TIMEOUT_SECONDS,
            }
        }