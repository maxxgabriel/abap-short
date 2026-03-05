"""
ETL Configuration Module
Converts ABAP constants to Python configuration with static attributes.
Maps ABAP types (i, p) to Python int/Decimal and structures to dictionaries/Enums.
"""

from decimal import Decimal
from enum import Enum
from typing import Dict, Any
from dataclasses import dataclass


class Status(Enum):
    """Status codes mapping from ABAP gc_status structure"""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process steps mapping from ABAP gc_step structure"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class SaleCategory(Enum):
    """Sale categories mapping from ABAP gc_category structure"""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


@dataclass(frozen=True)
class BusinessRules:
    """Business rules constants - immutable configuration"""
    # Discount thresholds (ABAP TYPE i -> Python int)
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    
    # Discount rates (ABAP TYPE p DECIMALS 2 -> Python Decimal)
    DISCOUNT_RATE_TIER1: Decimal = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Decimal = Decimal('0.10')
    
    # Tax rate (ABAP TYPE p DECIMALS 2 -> Python Decimal)
    TAX_RATE: Decimal = Decimal('0.08')
    
    # Cost ratio (ABAP TYPE p DECIMALS 2 -> Python Decimal)
    COST_RATIO: Decimal = Decimal('0.60')
    
    # Category thresholds (ABAP TYPE p DECIMALS 2 -> Python Decimal)
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal('500.00')


@dataclass(frozen=True)
class ETLDefaults:
    """ETL configuration defaults - immutable configuration"""
    # All ABAP TYPE i -> Python int
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600


@dataclass(frozen=True)
class IDPrefixes:
    """ID prefixes for generating unique identifiers"""
    ETL_RUN: str = 'ETL'
    LOG_ID: str = 'LOG'
    ANALYTICS_ID: str = 'ANL'


@dataclass(frozen=True)
class Messages:
    """Standard message texts for logging"""
    INIT_SUCCESS: str = 'ETL process initialized successfully'
    EXTRACT_START: str = 'Starting data extraction'
    EXTRACT_COMPLETE: str = 'Data extraction completed'
    TRANSFORM_START: str = 'Starting data transformation'
    TRANSFORM_COMPLETE: str = 'Data transformation completed'
    LOAD_START: str = 'Starting data load'
    LOAD_COMPLETE: str = 'Data load completed'
    ETL_COMPLETE: str = 'ETL process completed successfully'
    ETL_ERROR: str = 'ETL process failed'


class ETLConstants:
    """
    Main constants class for ETL system.
    Provides static access to all configuration constants.
    Equivalent to ABAP ZCL_ETL_CONSTANTS class.
    """
    
    # Status codes
    STATUS = Status
    
    # Process steps
    STEP = ProcessStep
    
    # Sale categories
    CATEGORY = SaleCategory
    
    # Business rules (frozen dataclass instance)
    BUSINESS_RULES = BusinessRules()
    
    # ETL defaults (frozen dataclass instance)
    ETL_DEFAULTS = ETLDefaults()
    
    # ID prefixes (frozen dataclass instance)
    ID_PREFIXES = IDPrefixes()
    
    # Messages (frozen dataclass instance)
    MESSAGES = Messages()
    
    @staticmethod
    def get_discount_rate(quantity: int) -> Decimal:
        """
        Calculate discount rate based on quantity.
        
        Args:
            quantity: Order quantity
            
        Returns:
            Applicable discount rate as Decimal
        """
        if quantity > ETLConstants.BUSINESS_RULES.DISCOUNT_QTY_TIER2:
            return ETLConstants.BUSINESS_RULES.DISCOUNT_RATE_TIER2
        elif quantity > ETLConstants.BUSINESS_RULES.DISCOUNT_QTY_TIER1:
            return ETLConstants.BUSINESS_RULES.DISCOUNT_RATE_TIER1
        return Decimal('0.00')
    
    @staticmethod
    def categorize_sale(gross_amount: Decimal) -> SaleCategory:
        """
        Categorize sale based on gross amount.
        
        Args:
            gross_amount: Gross transaction amount
            
        Returns:
            Sale category enum
        """
        if gross_amount >= ETLConstants.BUSINESS_RULES.CATEGORY_HIGH_THRESHOLD:
            return SaleCategory.HIGH
        elif gross_amount >= ETLConstants.BUSINESS_RULES.CATEGORY_MEDIUM_THRESHOLD:
            return SaleCategory.MEDIUM
        return SaleCategory.LOW
    
    @staticmethod
    def to_dict() -> Dict[str, Any]:
        """
        Convert all constants to dictionary format.
        Useful for configuration export or documentation.
        
        Returns:
            Dictionary representation of all constants
        """
        return {
            'status_codes': {s.name: s.value for s in Status},
            'process_steps': {s.name: s.value for s in ProcessStep},
            'sale_categories': {c.name: c.value for c in SaleCategory},
            'business_rules': {
                'discount_qty_tier1': ETLConstants.BUSINESS_RULES.DISCOUNT_QTY_TIER1,
                'discount_qty_tier2': ETLConstants.BUSINESS_RULES.DISCOUNT_QTY_TIER2,
                'discount_rate_tier1': str(ETLConstants.BUSINESS_RULES.DISCOUNT_RATE_TIER1),
                'discount_rate_tier2': str(ETLConstants.BUSINESS_RULES.DISCOUNT_RATE_TIER2),
                'tax_rate': str(ETLConstants.BUSINESS_RULES.TAX_RATE),
                'cost_ratio': str(ETLConstants.BUSINESS_RULES.COST_RATIO),
                'category_high_threshold': str(ETLConstants.BUSINESS_RULES.CATEGORY_HIGH_THRESHOLD),
                'category_medium_threshold': str(ETLConstants.BUSINESS_RULES.CATEGORY_MEDIUM_THRESHOLD),
            },
            'etl_defaults': {
                'default_batch_size': ETLConstants.ETL_DEFAULTS.DEFAULT_BATCH_SIZE,
                'default_commit_interval': ETLConstants.ETL_DEFAULTS.DEFAULT_COMMIT_INTERVAL,
                'default_retry_attempts': ETLConstants.ETL_DEFAULTS.DEFAULT_RETRY_ATTEMPTS,
                'default_timeout_seconds': ETLConstants.ETL_DEFAULTS.DEFAULT_TIMEOUT_SECONDS,
            },
            'id_prefixes': {
                'etl_run': ETLConstants.ID_PREFIXES.ETL_RUN,
                'log_id': ETLConstants.ID_PREFIXES.LOG_ID,
                'analytics_id': ETLConstants.ID_PREFIXES.ANALYTICS_ID,
            },
            'messages': {
                'init_success': ETLConstants.MESSAGES.INIT_SUCCESS,
                'extract_start': ETLConstants.MESSAGES.EXTRACT_START,
                'extract_complete': ETLConstants.MESSAGES.EXTRACT_COMPLETE,
                'transform_start': ETLConstants.MESSAGES.TRANSFORM_START,
                'transform_complete': ETLConstants.MESSAGES.TRANSFORM_COMPLETE,
                'load_start': ETLConstants.MESSAGES.LOAD_START,
                'load_complete': ETLConstants.MESSAGES.LOAD_COMPLETE,
                'etl_complete': ETLConstants.MESSAGES.ETL_COMPLETE,
                'etl_error': ETLConstants.MESSAGES.ETL_ERROR,
            }
        }


# Module-level convenience constants for backward compatibility
GC_STATUS = Status
GC_STEP = ProcessStep
GC_CATEGORY = SaleCategory