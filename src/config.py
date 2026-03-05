"""
Configuration module for Sales ETL System.
Migrated from ABAP ZETL_TOP include and ZCL_ETL_CONSTANTS class.
"""

from dataclasses import dataclass
from typing import Dict, Any
from decimal import Decimal


@dataclass
class StatusCodes:
    """ETL status codes."""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass
class ProcessSteps:
    """ETL process step identifiers."""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass
class SaleCategories:
    """Sale categorization values."""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'


@dataclass
class BusinessRules:
    """Business rules and thresholds for ETL transformations."""
    # Discount thresholds
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


@dataclass
class ETLConfig:
    """ETL runtime configuration parameters."""
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN: str = 'ETL'
    PREFIX_LOG_ID: str = 'LOG'
    PREFIX_ANALYTICS_ID: str = 'ANL'
    
    # Operational parameters
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    test_mode: bool = False


@dataclass
class MessageTemplates:
    """Standard message templates for ETL logging."""
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
    Main configuration class containing all ETL constants and configurations.
    Migrated from ABAP ZCL_ETL_CONSTANTS and ZETL_TOP.
    """
    
    status = StatusCodes()
    steps = ProcessSteps()
    categories = SaleCategories()
    rules = BusinessRules()
    config = ETLConfig()
    messages = MessageTemplates()
    
    @classmethod
    def get_config_dict(cls) -> Dict[str, Any]:
        """Return configuration as dictionary for serialization."""
        return {
            'batch_size': cls.config.batch_size,
            'commit_interval': cls.config.commit_interval,
            'retry_attempts': cls.config.retry_attempts,
            'timeout_seconds': cls.config.timeout_seconds,
            'test_mode': cls.config.test_mode,
            'discount_qty_tier1': int(cls.rules.DISCOUNT_QTY_TIER1),
            'discount_qty_tier2': int(cls.rules.DISCOUNT_QTY_TIER2),
            'discount_rate_tier1': float(cls.rules.DISCOUNT_RATE_TIER1),
            'discount_rate_tier2': float(cls.rules.DISCOUNT_RATE_TIER2),
            'tax_rate': float(cls.rules.TAX_RATE),
            'cost_ratio': float(cls.rules.COST_RATIO),
            'category_high_threshold': float(cls.rules.CATEGORY_HIGH_THRESHOLD),
            'category_medium_threshold': float(cls.rules.CATEGORY_MEDIUM_THRESHOLD),
        }
    
    @classmethod
    def update_from_dict(cls, config_dict: Dict[str, Any]) -> None:
        """Update configuration from dictionary (e.g., loaded from YAML)."""
        if 'batch_size' in config_dict:
            cls.config.batch_size = config_dict['batch_size']
        if 'commit_interval' in config_dict:
            cls.config.commit_interval = config_dict['commit_interval']
        if 'retry_attempts' in config_dict:
            cls.config.retry_attempts = config_dict['retry_attempts']
        if 'timeout_seconds' in config_dict:
            cls.config.timeout_seconds = config_dict['timeout_seconds']
        if 'test_mode' in config_dict:
            cls.config.test_mode = config_dict['test_mode']
        
        # Update business rules if provided
        if 'discount_qty_tier1' in config_dict:
            cls.rules.DISCOUNT_QTY_TIER1 = config_dict['discount_qty_tier1']
        if 'discount_qty_tier2' in config_dict:
            cls.rules.DISCOUNT_QTY_TIER2 = config_dict['discount_qty_tier2']
        if 'discount_rate_tier1' in config_dict:
            cls.rules.DISCOUNT_RATE_TIER1 = Decimal(str(config_dict['discount_rate_tier1']))
        if 'discount_rate_tier2' in config_dict:
            cls.rules.DISCOUNT_RATE_TIER2 = Decimal(str(config_dict['discount_rate_tier2']))
        if 'tax_rate' in config_dict:
            cls.rules.TAX_RATE = Decimal(str(config_dict['tax_rate']))
        if 'cost_ratio' in config_dict:
            cls.rules.COST_RATIO = Decimal(str(config_dict['cost_ratio']))
        if 'category_high_threshold' in config_dict:
            cls.rules.CATEGORY_HIGH_THRESHOLD = Decimal(str(config_dict['category_high_threshold']))
        if 'category_medium_threshold' in config_dict:
            cls.rules.CATEGORY_MEDIUM_THRESHOLD = Decimal(str(config_dict['category_medium_threshold']))


# Global statistics tracking (migrated from ZETL_TOP)
@dataclass
class ETLStatistics:
    """Global statistics for ETL execution."""
    total_extracted: int = 0
    total_transformed: int = 0
    total_loaded: int = 0
    errors_count: int = 0
    warnings_count: int = 0
    
    def reset(self) -> None:
        """Reset all statistics to zero."""
        self.total_extracted = 0
        self.total_transformed = 0
        self.total_loaded = 0
        self.errors_count = 0
        self.warnings_count = 0
    
    def to_dict(self) -> Dict[str, int]:
        """Convert statistics to dictionary."""
        return {
            'total_extracted': self.total_extracted,
            'total_transformed': self.total_transformed,
            'total_loaded': self.total_loaded,
            'errors_count': self.errors_count,
            'warnings_count': self.warnings_count,
        }