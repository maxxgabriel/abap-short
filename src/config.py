"""
ETL Configuration Module
Centralized configuration and constants for the Sales ETL system.
Converted from ABAP ZCL_ETL_CONSTANTS class.
"""

from enum import Enum
from decimal import Decimal
from typing import Dict, Any
from dataclasses import dataclass


class StatusCode(Enum):
    """Status codes for ETL process tracking"""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process steps"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class SaleCategory(Enum):
    """Sale categorization levels"""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


@dataclass(frozen=True)
class BusinessRules:
    """Business rules constants for calculations"""
    # Discount thresholds and rates
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Decimal = Decimal('0.10')
    
    # Tax rate
    TAX_RATE: Decimal = Decimal('0.08')
    
    # Cost ratio for profit calculation
    COST_RATIO: Decimal = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal('500.00')


@dataclass(frozen=True)
class ETLConfig:
    """ETL processing configuration defaults"""
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600


@dataclass(frozen=True)
class IDPrefixes:
    """ID generation prefixes"""
    ETL_RUN: str = 'ETL'
    LOG_ID: str = 'LOG'
    ANALYTICS_ID: str = 'ANL'


@dataclass(frozen=True)
class MessageTexts:
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
    Provides static access to all configuration and constants.
    Converted from ABAP ZCL_ETL_CONSTANTS.
    """
    
    # Status codes
    STATUS = StatusCode
    
    # Process steps
    STEP = ProcessStep
    
    # Sale categories
    CATEGORY = SaleCategory
    
    # Business rules
    BUSINESS_RULES = BusinessRules()
    
    # ETL configuration
    CONFIG = ETLConfig()
    
    # ID prefixes
    PREFIXES = IDPrefixes()
    
    # Messages
    MESSAGES = MessageTexts()
    
    @staticmethod
    def get_discount_rate(quantity: int) -> Decimal:
        """
        Calculate discount rate based on quantity.
        
        Args:
            quantity: Order quantity
            
        Returns:
            Applicable discount rate as Decimal
        """
        rules = ETLConstants.BUSINESS_RULES
        if quantity > rules.DISCOUNT_QTY_TIER2:
            return rules.DISCOUNT_RATE_TIER2
        elif quantity > rules.DISCOUNT_QTY_TIER1:
            return rules.DISCOUNT_RATE_TIER1
        else:
            return Decimal('0.00')
    
    @staticmethod
    def categorize_sale(gross_amount: Decimal) -> SaleCategory:
        """
        Categorize sale based on gross amount.
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Sale category enum value
        """
        rules = ETLConstants.BUSINESS_RULES
        if gross_amount >= rules.CATEGORY_HIGH_THRESHOLD:
            return SaleCategory.HIGH
        elif gross_amount >= rules.CATEGORY_MEDIUM_THRESHOLD:
            return SaleCategory.MEDIUM
        else:
            return SaleCategory.LOW
    
    @staticmethod
    def to_dict() -> Dict[str, Any]:
        """
        Convert all constants to dictionary format.
        
        Returns:
            Dictionary with all configuration values
        """
        return {
            'status_codes': {s.name: s.value for s in StatusCode},
            'process_steps': {s.name: s.value for s in ProcessStep},
            'sale_categories': {s.name: s.value for s in SaleCategory},
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
            'etl_config': {
                'default_batch_size': ETLConstants.CONFIG.DEFAULT_BATCH_SIZE,
                'default_commit_interval': ETLConstants.CONFIG.DEFAULT_COMMIT_INTERVAL,
                'default_retry_attempts': ETLConstants.CONFIG.DEFAULT_RETRY_ATTEMPTS,
                'default_timeout_seconds': ETLConstants.CONFIG.DEFAULT_TIMEOUT_SECONDS,
            },
            'prefixes': {
                'etl_run': ETLConstants.PREFIXES.ETL_RUN,
                'log_id': ETLConstants.PREFIXES.LOG_ID,
                'analytics_id': ETLConstants.PREFIXES.ANALYTICS_ID,
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


# Convenience aliases for backward compatibility
GC_STATUS = StatusCode
GC_STEP = ProcessStep
GC_CATEGORY = SaleCategory