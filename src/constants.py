"""
Constants and configuration values for ETL system.
"""
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class StatusCodes:
    """ETL status codes."""
    NEW: str = "N"
    PROCESSED: str = "P"
    ERROR: str = "E"
    WARNING: str = "W"
    SUCCESS: str = "S"
    INFO: str = "I"


@dataclass(frozen=True)
class ProcessSteps:
    """ETL process step identifiers."""
    INIT: str = "INIT"
    EXTRACT: str = "EXTRACT"
    TRANSFORM: str = "TRANSFORM"
    LOAD: str = "LOAD"
    VALIDATE: str = "VALIDATE"
    COMPLETE: str = "COMPLETE"
    ERROR: str = "ERROR"


@dataclass(frozen=True)
class Categories:
    """Sale category classifications."""
    HIGH: str = "HIGH"
    MEDIUM: str = "MEDIUM"
    LOW: str = "LOW"


class ETLConstants:
    """Central constants for ETL system."""
    
    STATUS = StatusCodes()
    STEPS = ProcessSteps()
    CATEGORIES = Categories()
    
    # Business Rules - Discount
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = 0.05
    DISCOUNT_RATE_TIER2 = 0.10
    
    # Business Rules - Tax and Cost
    TAX_RATE = 0.08
    COST_RATIO = 0.60
    
    # Business Rules - Category Thresholds
    CATEGORY_HIGH_THRESHOLD = 2000.00
    CATEGORY_MEDIUM_THRESHOLD = 500.00
    
    # Processing Configuration
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID Prefixes
    PREFIX_ETL_RUN = "ETL"
    PREFIX_LOG_ID = "LOG"
    PREFIX_ANALYTICS_ID = "ANL"
    
    @classmethod
    def get_business_rules(cls) -> Dict:
        """Get all business rules as dictionary."""
        return {
            "discount": {
                "tier1_quantity": cls.DISCOUNT_QTY_TIER1,
                "tier2_quantity": cls.DISCOUNT_QTY_TIER2,
                "tier1_rate": cls.DISCOUNT_RATE_TIER1,
                "tier2_rate": cls.DISCOUNT_RATE_TIER2
            },
            "tax_rate": cls.TAX_RATE,
            "cost_ratio": cls.COST_RATIO,
            "category_thresholds": {
                "high": cls.CATEGORY_HIGH_THRESHOLD,
                "medium": cls.CATEGORY_MEDIUM_THRESHOLD
            }
        }