"""
ETL Constants and Configuration Module

This module provides centralized constants and configuration values
for the Sales ETL system, migrated from ABAP ZCL_ETL_CONSTANTS.
"""

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class StatusCodes:
    """ETL process status codes"""
    NEW: Final[str] = 'N'
    PROCESSED: Final[str] = 'P'
    ERROR: Final[str] = 'E'
    WARNING: Final[str] = 'W'
    SUCCESS: Final[str] = 'S'
    INFO: Final[str] = 'I'


@dataclass(frozen=True)
class ProcessSteps:
    """ETL process step identifiers"""
    INIT: Final[str] = 'INIT'
    EXTRACT: Final[str] = 'EXTRACT'
    TRANSFORM: Final[str] = 'TRANSFORM'
    LOAD: Final[str] = 'LOAD'
    VALIDATE: Final[str] = 'VALIDATE'
    COMPLETE: Final[str] = 'COMPLETE'
    ERROR: Final[str] = 'ERROR'


@dataclass(frozen=True)
class SaleCategories:
    """Sale categorization values"""
    HIGH: Final[str] = 'HIGH'
    MEDIUM: Final[str] = 'MEDIUM'
    LOW: Final[str] = 'LOW'


@dataclass(frozen=True)
class BusinessRules:
    """Business calculation rules and thresholds"""
    # Discount thresholds and rates
    DISCOUNT_QTY_TIER1: Final[int] = 10
    DISCOUNT_QTY_TIER2: Final[int] = 15
    DISCOUNT_RATE_TIER1: Final[float] = 0.05
    DISCOUNT_RATE_TIER2: Final[float] = 0.10
    
    # Tax rate
    TAX_RATE: Final[float] = 0.08
    
    # Cost calculation
    COST_RATIO: Final[float] = 0.60
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Final[float] = 2000.00
    CATEGORY_MEDIUM_THRESHOLD: Final[float] = 500.00


@dataclass(frozen=True)
class ETLDefaults:
    """Default ETL configuration values"""
    BATCH_SIZE: Final[int] = 1000
    COMMIT_INTERVAL: Final[int] = 500
    RETRY_ATTEMPTS: Final[int] = 3
    TIMEOUT_SECONDS: Final[int] = 3600


@dataclass(frozen=True)
class IDPrefixes:
    """ID generation prefixes"""
    ETL_RUN: Final[str] = 'ETL'
    LOG_ID: Final[str] = 'LOG'
    ANALYTICS_ID: Final[str] = 'ANL'


@dataclass(frozen=True)
class Messages:
    """Standard ETL messages"""
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
    """Main constants container class"""
    
    STATUS = StatusCodes()
    STEP = ProcessSteps()
    CATEGORY = SaleCategories()
    RULES = BusinessRules()
    DEFAULTS = ETLDefaults()
    PREFIX = IDPrefixes()
    MSG = Messages()
    
    @classmethod
    def get_all_constants(cls) -> dict:
        """Return all constants as a dictionary for validation"""
        return {
            'status': vars(cls.STATUS),
            'steps': vars(cls.STEP),
            'categories': vars(cls.CATEGORY),
            'rules': vars(cls.RULES),
            'defaults': vars(cls.DEFAULTS),
            'prefixes': vars(cls.PREFIX),
            'messages': vars(cls.MSG)
        }