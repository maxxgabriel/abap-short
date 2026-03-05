"""
Configuration module for Sales ETL System.
Converts ABAP constants to Python configuration with type-safe enums and Decimal precision.
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional


class ETLStatus(Enum):
    """ETL status codes mapping from ABAP gc_status."""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ETLStep(Enum):
    """ETL process steps mapping from ABAP gc_step."""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategory(Enum):
    """Sale categories mapping from ABAP gc_category."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True)
class BusinessRules:
    """Business rules for ETL transformation with Decimal precision."""
    
    # Discount thresholds and rates
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal("0.05")
    DISCOUNT_RATE_TIER2: Decimal = Decimal("0.10")
    
    # Tax rate
    TAX_RATE: Decimal = Decimal("0.08")
    
    # Cost ratio for profit margin calculation
    COST_RATIO: Decimal = Decimal("0.60")
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal("500.00")


@dataclass(frozen=True)
class ETLConfig:
    """ETL runtime configuration parameters."""
    
    # Batch processing configuration
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN: str = "ETL"
    PREFIX_LOG_ID: str = "LOG"
    PREFIX_ANALYTICS_ID: str = "ANL"


@dataclass(frozen=True)
class ETLMessages:
    """Standard ETL process messages."""
    
    # Initialization messages
    INIT_SUCCESS: str = "ETL process initialized successfully"
    
    # Extraction messages
    EXTRACT_START: str = "Starting data extraction"
    EXTRACT_COMPLETE: str = "Data extraction completed"
    
    # Transformation messages
    TRANSFORM_START: str = "Starting data transformation"
    TRANSFORM_COMPLETE: str = "Data transformation completed"
    
    # Load messages
    LOAD_START: str = "Starting data load"
    LOAD_COMPLETE: str = "Data load completed"
    
    # Completion messages
    ETL_COMPLETE: str = "ETL process completed successfully"
    ETL_ERROR: str = "ETL process failed"


class ETLConstants:
    """
    Main constants class combining all ETL configuration.
    Singleton pattern for global access.
    """
    
    _instance: Optional['ETLConstants'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialize all constant groups."""
        self.status = ETLStatus
        self.step = ETLStep
        self.category = SaleCategory
        self.business_rules = BusinessRules()
        self.config = ETLConfig()
        self.messages = ETLMessages()
    
    @classmethod
    def get_instance(cls) -> 'ETLConstants':
        """Get singleton instance of constants."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


# Global constants instance for easy import
CONSTANTS = ETLConstants.get_instance()

# Convenience exports
__all__ = [
    'ETLStatus',
    'ETLStep',
    'SaleCategory',
    'BusinessRules',
    'ETLConfig',
    'ETLMessages',
    'ETLConstants',
    'CONSTANTS',
]