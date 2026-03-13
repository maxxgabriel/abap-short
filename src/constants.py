"""
ETL Constants migrated from ABAP ZCL_ETL_CONSTANTS class.

Contains all status codes, step identifiers, category values,
and business rule constants used throughout the ETL pipeline.
"""

from decimal import Decimal
from typing import Dict, Any


class ETLStatus:
    """Status codes for ETL processing (ABAP: gc_status)."""
    
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ETLStep:
    """ETL process step identifiers (ABAP: gc_step)."""
    
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategory:
    """Sale categorization values (ABAP: gc_category)."""
    
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class BusinessRules:
    """Business rules and thresholds for ETL calculations."""
    
    # Discount thresholds and rates
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal("0.05")  # 5%
    DISCOUNT_RATE_TIER2 = Decimal("0.10")  # 10%
    
    # Tax rate
    TAX_RATE = Decimal("0.08")  # 8%
    
    # Cost ratio for profit calculation
    COST_RATIO = Decimal("0.60")  # 60% of unit price
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD = Decimal("500.00")


class ETLConfig:
    """Default ETL configuration parameters."""
    
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    DEFAULT_PARALLEL_JOBS = 4


class IDPrefixes:
    """ID generation prefixes."""
    
    ETL_RUN = "ETL"
    LOG_ID = "LOG"
    ANALYTICS_ID = "ANL"


class Messages:
    """Standard message texts for logging."""
    
    INIT_SUCCESS = "ETL process initialized successfully"
    EXTRACT_START = "Starting data extraction"
    EXTRACT_COMPLETE = "Data extraction completed"
    TRANSFORM_START = "Starting data transformation"
    TRANSFORM_COMPLETE = "Data transformation completed"
    LOAD_START = "Starting data load"
    LOAD_COMPLETE = "Data load completed"
    ETL_COMPLETE = "ETL process completed successfully"
    ETL_ERROR = "ETL process failed"


class Constants:
    """
    Unified constants class providing all configuration values.
    Matches ABAP ZCL_ETL_CONSTANTS structure.
    """
    
    status = ETLStatus
    step = ETLStep
    category = SaleCategory
    rules = BusinessRules
    config = ETLConfig
    prefixes = IDPrefixes
    messages = Messages
    
    @classmethod
    def get_all_constants(cls) -> Dict[str, Any]:
        """
        Return dictionary of all constants for configuration export.
        
        Returns:
            Dictionary with all constant values
        """
        return {
            "status": {
                "new": cls.status.NEW,
                "processed": cls.status.PROCESSED,
                "error": cls.status.ERROR,
                "warning": cls.status.WARNING,
                "success": cls.status.SUCCESS,
                "info": cls.status.INFO,
            },
            "step": {
                "init": cls.step.INIT,
                "extract": cls.step.EXTRACT,
                "transform": cls.step.TRANSFORM,
                "load": cls.step.LOAD,
                "validate": cls.step.VALIDATE,
                "complete": cls.step.COMPLETE,
                "error": cls.step.ERROR,
            },
            "category": {
                "high": cls.category.HIGH,
                "medium": cls.category.MEDIUM,
                "low": cls.category.LOW,
            },
            "business_rules": {
                "discount_qty_tier1": cls.rules.DISCOUNT_QTY_TIER1,
                "discount_qty_tier2": cls.rules.DISCOUNT_QTY_TIER2,
                "discount_rate_tier1": float(cls.rules.DISCOUNT_RATE_TIER1),
                "discount_rate_tier2": float(cls.rules.DISCOUNT_RATE_TIER2),
                "tax_rate": float(cls.rules.TAX_RATE),
                "cost_ratio": float(cls.rules.COST_RATIO),
                "category_high_threshold": float(cls.rules.CATEGORY_HIGH_THRESHOLD),
                "category_medium_threshold": float(cls.rules.CATEGORY_MEDIUM_THRESHOLD),
            },
            "etl_config": {
                "batch_size": cls.config.DEFAULT_BATCH_SIZE,
                "commit_interval": cls.config.DEFAULT_COMMIT_INTERVAL,
                "retry_attempts": cls.config.DEFAULT_RETRY_ATTEMPTS,
                "timeout_seconds": cls.config.DEFAULT_TIMEOUT_SECONDS,
                "parallel_jobs": cls.config.DEFAULT_PARALLEL_JOBS,
            },
            "id_prefixes": {
                "etl_run": cls.prefixes.ETL_RUN,
                "log_id": cls.prefixes.LOG_ID,
                "analytics_id": cls.prefixes.ANALYTICS_ID,
            },
        }


# Module-level convenience exports
STATUS = ETLStatus
STEP = ETLStep
CATEGORY = SaleCategory
RULES = BusinessRules
CONFIG = ETLConfig
PREFIXES = IDPrefixes
MESSAGES = Messages