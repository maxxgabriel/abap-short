"""
ETL Constants Module
Migrated from ZCL_ETL_CONSTANTS ABAP class
Provides centralized configuration and constants for the ETL system
"""
from dataclasses import dataclass
from typing import Dict
import yaml
from pathlib import Path


@dataclass(frozen=True)
class StatusCodes:
    """Status codes for ETL processes"""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass(frozen=True)
class ProcessSteps:
    """ETL process steps"""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass(frozen=True)
class Categories:
    """Sale categories"""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'


@dataclass(frozen=True)
class BusinessRules:
    """Business rules for transformations"""
    # Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: float = 0.05
    DISCOUNT_RATE_TIER2: float = 0.10
    
    # Tax rate
    TAX_RATE: float = 0.08
    
    # Cost ratio
    COST_RATIO: float = 0.60
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: float = 2000.00
    CATEGORY_MEDIUM_THRESHOLD: float = 500.00


@dataclass(frozen=True)
class ETLConfig:
    """ETL configuration defaults"""
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_COMMIT_INTERVAL: int = 500
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 3600


@dataclass(frozen=True)
class Prefixes:
    """ID prefixes"""
    ETL_RUN: str = 'ETL'
    LOG_ID: str = 'LOG'
    ANALYTICS_ID: str = 'ANL'


class ETLConstants:
    """
    Main constants class for ETL system
    Provides static access to all configuration values
    """
    
    _config: Dict = None
    
    # Static instances
    STATUS = StatusCodes()
    STEPS = ProcessSteps()
    CATEGORIES = Categories()
    RULES = BusinessRules()
    CONFIG = ETLConfig()
    PREFIXES = Prefixes()
    
    # Message texts
    MESSAGES = {
        'init_success': 'ETL process initialized successfully',
        'extract_start': 'Starting data extraction',
        'extract_complete': 'Data extraction completed',
        'transform_start': 'Starting data transformation',
        'transform_complete': 'Data transformation completed',
        'load_start': 'Starting data load',
        'load_complete': 'Data load completed',
        'etl_complete': 'ETL process completed successfully',
        'etl_error': 'ETL process failed'
    }
    
    @classmethod
    def load_config(cls, config_path: str = "config.yaml") -> Dict:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        if cls._config is None:
            config_file = Path(config_path)
            if config_file.exists():
                with open(config_file, 'r') as f:
                    cls._config = yaml.safe_load(f)
            else:
                cls._config = {}
        return cls._config
    
    @classmethod
    def get_config_value(cls, key_path: str, default=None):
        """
        Get configuration value by dot-notation path
        
        Args:
            key_path: Dot-separated path (e.g., 'discount.rate_tier1')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        if cls._config is None:
            cls.load_config()
        
        keys = key_path.split('.')
        value = cls._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value