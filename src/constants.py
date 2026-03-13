"""
ETL Constants Module
Defines all constants and configuration values for the ETL system
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Final


@dataclass(frozen=True)
class StatusCodes:
    """Status codes for ETL processing"""
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
    """Sale category classifications"""
    HIGH: Final[str] = 'HIGH'
    MEDIUM: Final[str] = 'MEDIUM'
    LOW: Final[str] = 'LOW'


@dataclass(frozen=True)
class BusinessRules:
    """Business rules and thresholds"""
    # Discount thresholds
    DISCOUNT_QTY_TIER1: Final[int] = 10
    DISCOUNT_QTY_TIER2: Final[int] = 15
    DISCOUNT_RATE_TIER1: Final[Decimal] = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Final[Decimal] = Decimal('0.10')
    
    # Tax rate
    TAX_RATE: Final[Decimal] = Decimal('0.08')
    
    # Cost ratio
    COST_RATIO: Final[Decimal] = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Final[Decimal] = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Final[Decimal] = Decimal('500.00')


@dataclass(frozen=True)
class ETLConfiguration:
    """ETL configuration defaults"""
    DEFAULT_BATCH_SIZE: Final[int] = 1000
    DEFAULT_COMMIT_INTERVAL: Final[int] = 500
    DEFAULT_RETRY_ATTEMPTS: Final[int] = 3
    DEFAULT_TIMEOUT_SECONDS: Final[int] = 3600


@dataclass(frozen=True)
class IDPrefixes:
    """ID generation prefixes"""
    ETL_RUN: Final[str] = 'ETL'
    LOG_ID: Final[str] = 'LOG'
    ANALYTICS_ID: Final[str] = 'ANL'


@dataclass(frozen=True)
class Messages:
    """Standard message templates"""
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
    """Main constants container"""
    
    STATUS = StatusCodes()
    STEP = ProcessSteps()
    CATEGORY = SaleCategories()
    RULES = BusinessRules()
    CONFIG = ETLConfiguration()
    PREFIX = IDPrefixes()
    MSG = Messages()
    
    @classmethod
    def get_all_statuses(cls) -> list[str]:
        """Return all valid status codes"""
        return [
            cls.STATUS.NEW,
            cls.STATUS.PROCESSED,
            cls.STATUS.ERROR,
            cls.STATUS.WARNING,
            cls.STATUS.SUCCESS,
            cls.STATUS.INFO
        ]
    
    @classmethod
    def get_all_steps(cls) -> list[str]:
        """Return all valid process steps"""
        return [
            cls.STEP.INIT,
            cls.STEP.EXTRACT,
            cls.STEP.TRANSFORM,
            cls.STEP.LOAD,
            cls.STEP.VALIDATE,
            cls.STEP.COMPLETE,
            cls.STEP.ERROR
        ]
    
    @classmethod
    def get_all_categories(cls) -> list[str]:
        """Return all valid sale categories"""
        return [
            cls.CATEGORY.HIGH,
            cls.CATEGORY.MEDIUM,
            cls.CATEGORY.LOW
        ]


# Singleton instance for easy import
constants = ETLConstants()