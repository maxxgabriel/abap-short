"""
Configuration module for Sales ETL System
Migrated from ABAP ZCL_ETL_CONSTANTS
"""

import os
from typing import Dict, Any
from pathlib import Path
import yaml
from dataclasses import dataclass, field


@dataclass
class StatusCodes:
    """Status code constants"""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass
class ProcessSteps:
    """ETL process step constants"""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass
class Categories:
    """Sale category constants"""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'


@dataclass
class DiscountRules:
    """Discount business rules"""
    tier1_quantity_threshold: int = 10
    tier2_quantity_threshold: int = 15
    tier1_rate: float = 0.05
    tier2_rate: float = 0.10


@dataclass
class BusinessRules:
    """Business rules configuration"""
    discount: DiscountRules = field(default_factory=DiscountRules)
    tax_rate: float = 0.08
    cost_ratio: float = 0.60
    category_high_threshold: float = 2000.00
    category_medium_threshold: float = 500.00


@dataclass
class ETLDefaults:
    """ETL configuration defaults"""
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    parallel_jobs: int = 4
    test_mode: bool = False


@dataclass
class IDPrefixes:
    """ID prefix constants"""
    ETL_RUN: str = 'ETL'
    LOG: str = 'LOG'
    ANALYTICS: str = 'ANL'


class ETLConfig:
    """
    Main configuration class for ETL system
    Loads configuration from YAML file
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize configuration
        
        Args:
            config_path: Path to configuration YAML file
        """
        if config_path is None:
            config_path = os.getenv('ETL_CONFIG_PATH', 'config.yaml')
        
        self.config_path = Path(config_path)
        self._config_data = self._load_config()
        
        # Initialize constants
        self.status = StatusCodes()
        self.steps = ProcessSteps()
        self.categories = Categories()
        self.id_prefixes = IDPrefixes()
        
        # Load business rules
        self.business_rules = self._load_business_rules()
        
        # Load ETL defaults
        self.etl_defaults = self._load_etl_defaults()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _load_business_rules(self) -> BusinessRules:
        """Load business rules from configuration"""
        rules_config = self._config_data.get('business_rules', {})
        
        discount_config = rules_config.get('discount', {})
        discount = DiscountRules(
            tier1_quantity_threshold=discount_config.get('tier1_quantity_threshold', 10),
            tier2_quantity_threshold=discount_config.get('tier2_quantity_threshold', 15),
            tier1_rate=discount_config.get('tier1_rate', 0.05),
            tier2_rate=discount_config.get('tier2_rate', 0.10)
        )
        
        return BusinessRules(
            discount=discount,
            tax_rate=rules_config.get('tax', {}).get('rate', 0.08),
            cost_ratio=rules_config.get('cost', {}).get('ratio', 0.60),
            category_high_threshold=rules_config.get('category_thresholds', {}).get('high', 2000.00),
            category_medium_threshold=rules_config.get('category_thresholds', {}).get('medium', 500.00)
        )
    
    def _load_etl_defaults(self) -> ETLDefaults:
        """Load ETL default configuration"""
        etl_config = self._config_data.get('etl_config', {})
        
        return ETLDefaults(
            batch_size=etl_config.get('batch_size', 1000),
            commit_interval=etl_config.get('commit_interval', 500),
            retry_attempts=etl_config.get('retry_attempts', 3),
            timeout_seconds=etl_config.get('timeout_seconds', 3600),
            parallel_jobs=etl_config.get('parallel_jobs', 4),
            test_mode=etl_config.get('test_mode', False)
        )
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration"""
        return self._config_data.get('spark', {}).get('config', {})
    
    def get_data_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get data source configuration"""
        sources = self._config_data.get('data_sources', {})
        if source_name not in sources:
            raise ValueError(f"Data source not found: {source_name}")
        return sources[source_name]
    
    def get_message(self, message_key: str) -> str:
        """Get message template"""
        messages = self._config_data.get('messages', {})
        return messages.get(message_key, f"Message not found: {message_key}")
    
    def get_app_name(self) -> str:
        """Get application name"""
        return self._config_data.get('spark', {}).get('app_name', 'SalesETL')
    
    def get_master_url(self) -> str:
        """Get Spark master URL"""
        return self._config_data.get('spark', {}).get('master', 'local[*]')


# Global configuration instance
_global_config = None


def get_config(config_path: str = None) -> ETLConfig:
    """
    Get global configuration instance (singleton)
    
    Args:
        config_path: Path to configuration file (only used on first call)
    
    Returns:
        ETLConfig instance
    """
    global _global_config
    if _global_config is None:
        _global_config = ETLConfig(config_path)
    return _global_config