"""
ETL Configuration Module
Converts ABAP constants structure to Python configuration class
"""
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Dict


class StatusCode(Enum):
    """Status codes for ETL processes"""
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
    """Business rule constants for ETL transformations"""
    
    # Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Decimal = Decimal('0.10')
    
    # Tax configuration
    TAX_RATE: Decimal = Decimal('0.08')
    
    # Cost calculation
    COST_RATIO: Decimal = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal('500.00')


@dataclass(frozen=True)
class ETLConfiguration:
    """ETL system configuration defaults"""
    
    # Batch processing settings
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN: str = 'ETL'
    PREFIX_LOG_ID: str = 'LOG'
    PREFIX_ANALYTICS_ID: str = 'ANL'


@dataclass(frozen=True)
class MessageTemplates:
    """Standard message templates for logging"""
    
    # Initialization messages
    MSG_INIT_SUCCESS: str = 'ETL process initialized successfully'
    
    # Extract phase messages
    MSG_EXTRACT_START: str = 'Starting data extraction'
    MSG_EXTRACT_COMPLETE: str = 'Data extraction completed'
    
    # Transform phase messages
    MSG_TRANSFORM_START: str = 'Starting data transformation'
    MSG_TRANSFORM_COMPLETE: str = 'Data transformation completed'
    
    # Load phase messages
    MSG_LOAD_START: str = 'Starting data load'
    MSG_LOAD_COMPLETE: str = 'Data load completed'
    
    # Completion messages
    MSG_ETL_COMPLETE: str = 'ETL process completed successfully'
    MSG_ETL_ERROR: str = 'ETL process failed'


class ETLConstants:
    """
    Main constants class for Sales ETL System
    Provides static access to all configuration, business rules, and constants
    """
    
    # Status and step enums
    Status = StatusCode
    Step = ProcessStep
    Category = SaleCategory
    
    # Configuration instances
    business_rules = BusinessRules()
    config = ETLConfiguration()
    messages = MessageTemplates()
    
    @staticmethod
    def get_status_dict() -> Dict[str, str]:
        """Get status codes as dictionary"""
        return {status.name: status.value for status in StatusCode}
    
    @staticmethod
    def get_step_dict() -> Dict[str, str]:
        """Get process steps as dictionary"""
        return {step.name: step.value for step in ProcessStep}
    
    @staticmethod
    def get_category_dict() -> Dict[str, str]:
        """Get sale categories as dictionary"""
        return {cat.name: cat.value for cat in SaleCategory}
    
    @staticmethod
    def get_discount_rate(quantity: int) -> Decimal:
        """
        Calculate discount rate based on quantity
        
        Args:
            quantity: Sales quantity
            
        Returns:
            Applicable discount rate as Decimal
        """
        rules = ETLConstants.business_rules
        
        if quantity > rules.DISCOUNT_QTY_TIER2:
            return rules.DISCOUNT_RATE_TIER2
        elif quantity > rules.DISCOUNT_QTY_TIER1:
            return rules.DISCOUNT_RATE_TIER1
        else:
            return Decimal('0.00')
    
    @staticmethod
    def categorize_sale(gross_amount: Decimal) -> str:
        """
        Categorize sale based on gross amount
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Category string (HIGH, MEDIUM, LOW)
        """
        rules = ETLConstants.business_rules
        
        if gross_amount >= rules.CATEGORY_HIGH_THRESHOLD:
            return SaleCategory.HIGH.value
        elif gross_amount >= rules.CATEGORY_MEDIUM_THRESHOLD:
            return SaleCategory.MEDIUM.value
        else:
            return SaleCategory.LOW.value
    
    @staticmethod
    def validate_status(status: str) -> bool:
        """Validate if status code is valid"""
        return status in [s.value for s in StatusCode]
    
    @staticmethod
    def validate_step(step: str) -> bool:
        """Validate if process step is valid"""
        return step in [s.value for s in ProcessStep]
    
    @staticmethod
    def validate_category(category: str) -> bool:
        """Validate if category is valid"""
        return category in [c.value for c in SaleCategory]


# Convenience exports
__all__ = [
    'ETLConstants',
    'StatusCode',
    'ProcessStep',
    'SaleCategory',
    'BusinessRules',
    'ETLConfiguration',
    'MessageTemplates'
]