"""
ETL Constants and Configuration
Migrated from ZCL_ETL_CONSTANTS
"""

from typing import Dict, Any
from dataclasses import dataclass
import yaml
from pathlib import Path


@dataclass(frozen=True)
class StatusCodes:
    """Status code constants"""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


@dataclass(frozen=True)
class ProcessSteps:
    """ETL process step constants"""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


@dataclass(frozen=True)
class SaleCategories:
    """Sale category constants"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True)
class IDPrefixes:
    """ID prefix constants"""
    ETL_RUN = "ETL"
    LOG_ID = "LOG"
    ANALYTICS_ID = "ANL"


class ETLConstants:
    """Main constants class for ETL system"""
    
    STATUS = StatusCodes()
    STEP = ProcessSteps()
    CATEGORY = SaleCategories()
    PREFIX = IDPrefixes()
    
    # Default Configuration Values
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize constants from configuration file
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config = self._load_config(config_path)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config_file = Path(config_path)
            if config_file.exists():
                with open(config_file, 'r') as f:
                    return yaml.safe_load(f)
            else:
                return self._get_default_config()
        except Exception as e:
            print(f"Warning: Could not load config from {config_path}: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            'etl': {
                'batch_size': self.DEFAULT_BATCH_SIZE,
                'commit_interval': self.DEFAULT_COMMIT_INTERVAL,
                'retry_attempts': self.DEFAULT_RETRY_ATTEMPTS,
                'timeout_seconds': self.DEFAULT_TIMEOUT_SECONDS,
            },
            'business_rules': {
                'discount': {
                    'tier1_quantity': 10,
                    'tier2_quantity': 15,
                    'tier1_rate': 0.05,
                    'tier2_rate': 0.10,
                },
                'tax_rate': 0.08,
                'cost_ratio': 0.60,
                'category': {
                    'high_threshold': 2000.00,
                    'medium_threshold': 500.00,
                }
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation path
        
        Args:
            key_path: Dot-separated path to config value (e.g., 'etl.batch_size')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
                
        return value


# Module-level constants instance
constants = ETLConstants()