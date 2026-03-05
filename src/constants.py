"""
Constants and configuration for the ETL system.

This module defines all business constants, status codes, and configuration
values used throughout the ETL process.
"""

from typing import Dict, Any
import yaml
from pathlib import Path


class ETLConstants:
    """Constants and configuration for ETL system"""
    
    # Status codes
    STATUS_NEW = "N"
    STATUS_PROCESSED = "P"
    STATUS_ERROR = "E"
    STATUS_WARNING = "W"
    STATUS_SUCCESS = "S"
    STATUS_INFO = "I"
    
    # ETL process steps
    STEP_INIT = "INIT"
    STEP_EXTRACT = "EXTRACT"
    STEP_TRANSFORM = "TRANSFORM"
    STEP_LOAD = "LOAD"
    STEP_VALIDATE = "VALIDATE"
    STEP_COMPLETE = "COMPLETE"
    STEP_ERROR = "ERROR"
    
    # Sale categories
    CATEGORY_HIGH = "HIGH"
    CATEGORY_MEDIUM = "MEDIUM"
    CATEGORY_LOW = "LOW"
    
    # Business rules - Discount thresholds (defaults)
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = 0.05
    DISCOUNT_RATE_TIER2 = 0.10
    
    # Business rules - Tax rate
    TAX_RATE = 0.08
    
    # Business rules - Cost ratio
    COST_RATIO = 0.60
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = 2000.00
    CATEGORY_MEDIUM_THRESHOLD = 500.00
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN = "ETL"
    PREFIX_LOG_ID = "LOG"
    PREFIX_ANALYTICS_ID = "ANL"
    
    # Message texts
    MSG_INIT_SUCCESS = "ETL process initialized successfully"
    MSG_EXTRACT_START = "Starting data extraction"
    MSG_EXTRACT_COMPLETE = "Data extraction completed"
    MSG_TRANSFORM_START = "Starting data transformation"
    MSG_TRANSFORM_COMPLETE = "Data transformation completed"
    MSG_LOAD_START = "Starting data load"
    MSG_LOAD_COMPLETE = "Data load completed"
    MSG_ETL_COMPLETE = "ETL process completed successfully"
    MSG_ETL_ERROR = "ETL process failed"
    
    _config: Dict[str, Any] = None
    
    @classmethod
    def load_config(cls, config_path: str = "config.yaml") -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
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
    def get_config_value(cls, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated path.
        
        Args:
            key_path: Dot-separated configuration key path (e.g., "business_rules.tax_rate")
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