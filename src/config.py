"""
ETL Configuration Module
Converts ABAP constants structure to Python configuration with static attributes.
Maps ABAP types (i, p) to Python int/Decimal and structures to dictionaries/Enums.
"""

from decimal import Decimal
from enum import Enum
from typing import Dict, Any
from dataclasses import dataclass, field
import yaml
from pathlib import Path


class StatusCode(Enum):
    """ETL Status codes - maps ABAP gc_status structure"""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL Process steps - maps ABAP gc_step structure"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class SaleCategory(Enum):
    """Sale categories - maps ABAP gc_category structure"""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


@dataclass(frozen=True)
class BusinessRules:
    """
    Business rules configuration.
    Maps ABAP constants with types:
    - i (integer) -> int
    - p LENGTH n DECIMALS m -> Decimal
    """
    # Discount thresholds (ABAP: TYPE i)
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    
    # Discount rates (ABAP: TYPE p LENGTH 3 DECIMALS 2)
    discount_rate_tier1: Decimal = Decimal('0.05')
    discount_rate_tier2: Decimal = Decimal('0.10')
    
    # Tax rate (ABAP: TYPE p LENGTH 3 DECIMALS 2)
    tax_rate: Decimal = Decimal('0.08')
    
    # Cost ratio (ABAP: TYPE p LENGTH 3 DECIMALS 2)
    cost_ratio: Decimal = Decimal('0.60')
    
    # Category thresholds (ABAP: TYPE p LENGTH 16 DECIMALS 2)
    category_high_threshold: Decimal = Decimal('2000.00')
    category_medium_threshold: Decimal = Decimal('500.00')


@dataclass(frozen=True)
class ETLDefaults:
    """
    ETL configuration defaults.
    All ABAP TYPE i -> Python int
    """
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    parallel_jobs: int = 4


@dataclass(frozen=True)
class IDPrefixes:
    """ID prefix constants - ABAP TYPE char3 -> str"""
    etl_run: str = 'ETL'
    log_id: str = 'LOG'
    analytics_id: str = 'ANL'


@dataclass(frozen=True)
class Messages:
    """Standard message texts - ABAP TYPE string -> str"""
    init_success: str = 'ETL process initialized successfully'
    extract_start: str = 'Starting data extraction'
    extract_complete: str = 'Data extraction completed'
    transform_start: str = 'Starting data transformation'
    transform_complete: str = 'Data transformation completed'
    load_start: str = 'Starting data load'
    load_complete: str = 'Data load completed'
    etl_complete: str = 'ETL process completed successfully'
    etl_error: str = 'ETL process failed'


class ETLConstants:
    """
    Main ETL Constants class - Python equivalent of ZCL_ETL_CONSTANTS.
    Uses static attributes and composition of config classes.
    Thread-safe and immutable by design.
    """
    
    # Status codes
    STATUS = StatusCode
    
    # Process steps
    STEP = ProcessStep
    
    # Sale categories
    CATEGORY = SaleCategory
    
    # Business rules
    BUSINESS_RULES = BusinessRules()
    
    # ETL defaults
    DEFAULTS = ETLDefaults()
    
    # ID prefixes
    PREFIXES = IDPrefixes()
    
    # Messages
    MESSAGES = Messages()
    
    @classmethod
    def get_status_dict(cls) -> Dict[str, str]:
        """Return status codes as dictionary"""
        return {status.name: status.value for status in cls.STATUS}
    
    @classmethod
    def get_step_dict(cls) -> Dict[str, str]:
        """Return process steps as dictionary"""
        return {step.name: step.value for step in cls.STEP}
    
    @classmethod
    def get_category_dict(cls) -> Dict[str, str]:
        """Return categories as dictionary"""
        return {cat.name: cat.value for cat in cls.CATEGORY}
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Export all constants as nested dictionary"""
        return {
            'status': cls.get_status_dict(),
            'step': cls.get_step_dict(),
            'category': cls.get_category_dict(),
            'business_rules': {
                'discount_qty_tier1': cls.BUSINESS_RULES.discount_qty_tier1,
                'discount_qty_tier2': cls.BUSINESS_RULES.discount_qty_tier2,
                'discount_rate_tier1': str(cls.BUSINESS_RULES.discount_rate_tier1),
                'discount_rate_tier2': str(cls.BUSINESS_RULES.discount_rate_tier2),
                'tax_rate': str(cls.BUSINESS_RULES.tax_rate),
                'cost_ratio': str(cls.BUSINESS_RULES.cost_ratio),
                'category_high_threshold': str(cls.BUSINESS_RULES.category_high_threshold),
                'category_medium_threshold': str(cls.BUSINESS_RULES.category_medium_threshold),
            },
            'defaults': {
                'batch_size': cls.DEFAULTS.batch_size,
                'commit_interval': cls.DEFAULTS.commit_interval,
                'retry_attempts': cls.DEFAULTS.retry_attempts,
                'timeout_seconds': cls.DEFAULTS.timeout_seconds,
                'parallel_jobs': cls.DEFAULTS.parallel_jobs,
            },
            'prefixes': {
                'etl_run': cls.PREFIXES.etl_run,
                'log_id': cls.PREFIXES.log_id,
                'analytics_id': cls.PREFIXES.analytics_id,
            },
            'messages': {
                'init_success': cls.MESSAGES.init_success,
                'extract_start': cls.MESSAGES.extract_start,
                'extract_complete': cls.MESSAGES.extract_complete,
                'transform_start': cls.MESSAGES.transform_start,
                'transform_complete': cls.MESSAGES.transform_complete,
                'load_start': cls.MESSAGES.load_start,
                'load_complete': cls.MESSAGES.load_complete,
                'etl_complete': cls.MESSAGES.etl_complete,
                'etl_error': cls.MESSAGES.etl_error,
            }
        }


@dataclass
class ETLConfig:
    """
    Runtime ETL configuration loaded from YAML.
    Allows override of defaults while maintaining immutable constants.
    """
    batch_size: int = field(default_factory=lambda: ETLConstants.DEFAULTS.batch_size)
    commit_interval: int = field(default_factory=lambda: ETLConstants.DEFAULTS.commit_interval)
    retry_attempts: int = field(default_factory=lambda: ETLConstants.DEFAULTS.retry_attempts)
    timeout_seconds: int = field(default_factory=lambda: ETLConstants.DEFAULTS.timeout_seconds)
    parallel_jobs: int = field(default_factory=lambda: ETLConstants.DEFAULTS.parallel_jobs)
    
    # Business rule overrides
    discount_qty_tier1: int = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.discount_qty_tier1)
    discount_qty_tier2: int = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.discount_qty_tier2)
    discount_rate_tier1: Decimal = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.discount_rate_tier1)
    discount_rate_tier2: Decimal = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.discount_rate_tier2)
    tax_rate: Decimal = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.tax_rate)
    cost_ratio: Decimal = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.cost_ratio)
    category_high_threshold: Decimal = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.category_high_threshold)
    category_medium_threshold: Decimal = field(default_factory=lambda: ETLConstants.BUSINESS_RULES.category_medium_threshold)
    
    # Spark configuration
    spark_app_name: str = "SalesETL"
    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = 200
    
    # Data paths
    raw_data_path: str = ""
    analytics_data_path: str = ""
    log_path: str = ""
    checkpoint_path: str = ""
    
    @classmethod
    def from_yaml(cls, config_path: str) -> 'ETLConfig':
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        # Convert string decimals to Decimal objects
        decimal_fields = [
            'discount_rate_tier1', 'discount_rate_tier2', 'tax_rate',
            'cost_ratio', 'category_high_threshold', 'category_medium_threshold'
        ]
        
        for field_name in decimal_fields:
            if field_name in config_dict:
                config_dict[field_name] = Decimal(str(config_dict[field_name]))
        
        return cls(**config_dict)
    
    def to_yaml(self, output_path: str) -> None:
        """Export configuration to YAML file"""
        config_dict = {
            'batch_size': self.batch_size,
            'commit_interval': self.commit_interval,
            'retry_attempts': self.retry_attempts,
            'timeout_seconds': self.timeout_seconds,
            'parallel_jobs': self.parallel_jobs,
            'discount_qty_tier1': self.discount_qty_tier1,
            'discount_qty_tier2': self.discount_qty_tier2,
            'discount_rate_tier1': str(self.discount_rate_tier1),
            'discount_rate_tier2': str(self.discount_rate_tier2),
            'tax_rate': str(self.tax_rate),
            'cost_ratio': str(self.cost_ratio),
            'category_high_threshold': str(self.category_high_threshold),
            'category_medium_threshold': str(self.category_medium_threshold),
            'spark_app_name': self.spark_app_name,
            'spark_master': self.spark_master,
            'spark_shuffle_partitions': self.spark_shuffle_partitions,
            'raw_data_path': self.raw_data_path,
            'analytics_data_path': self.analytics_data_path,
            'log_path': self.log_path,
            'checkpoint_path': self.checkpoint_path,
        }
        
        with open(output_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False)


# Singleton instance for global access (optional)
_config_instance: ETLConfig = None


def get_config(config_path: str = None) -> ETLConfig:
    """
    Get or create singleton ETL configuration instance.
    
    Args:
        config_path: Path to YAML config file (only used on first call)
    
    Returns:
        ETLConfig instance
    """
    global _config_instance
    
    if _config_instance is None:
        if config_path:
            _config_instance = ETLConfig.from_yaml(config_path)
        else:
            _config_instance = ETLConfig()
    
    return _config_instance


def reset_config() -> None:
    """Reset singleton configuration (useful for testing)"""
    global _config_instance
    _config_instance = None