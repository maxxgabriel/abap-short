"""
Configuration Manager Module
Handles centralized configuration management for the ETL system.
"""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
import logging


class ConfigManager:
    """
    Centralized configuration management for ETL processes.
    Loads configuration from YAML files and provides type-safe access.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        """Singleton pattern to ensure single configuration instance."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager."""
        if not self._config:
            self._load_config()
    
    def _load_config(self, config_path: Optional[str] = None) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file. If None, uses default.
        """
        if config_path is None:
            config_path = os.getenv('ETL_CONFIG_PATH', 'config.yaml')
        
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Validate required sections
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate that all required configuration sections exist."""
        required_sections = ['spark', 'database', 'etl', 'logging']
        missing = [section for section in required_sections if section not in self._config]
        
        if missing:
            raise ValueError(f"Missing required configuration sections: {', '.join(missing)}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key path (dot notation).
        
        Args:
            key: Configuration key path (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration section."""
        return self._config.get('spark', {})
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration section."""
        return self._config.get('database', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration section."""
        return self._config.get('etl', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration section."""
        return self._config.get('logging', {})
    
    def get_batch_size(self) -> int:
        """Get configured batch size for ETL processing."""
        return self.get('etl.batch_size', 1000)
    
    def get_commit_interval(self) -> int:
        """Get configured commit interval."""
        return self.get('etl.commit_interval', 500)
    
    def get_retry_attempts(self) -> int:
        """Get configured retry attempts."""
        return self.get('etl.retry_attempts', 3)
    
    def get_timeout_seconds(self) -> int:
        """Get configured timeout in seconds."""
        return self.get('etl.timeout_seconds', 3600)
    
    def get_category_thresholds(self) -> Dict[str, float]:
        """Get business rule category thresholds."""
        return {
            'high': self.get('business_rules.category.high_threshold', 2000.00),
            'medium': self.get('business_rules.category.medium_threshold', 500.00)
        }
    
    def get_discount_rules(self) -> Dict[str, Any]:
        """Get business rule discount configuration."""
        return {
            'tier1_quantity': self.get('business_rules.discount.tier1_quantity', 10),
            'tier2_quantity': self.get('business_rules.discount.tier2_quantity', 15),
            'tier1_rate': self.get('business_rules.discount.tier1_rate', 0.05),
            'tier2_rate': self.get('business_rules.discount.tier2_rate', 0.10)
        }
    
    def get_tax_rate(self) -> float:
        """Get configured tax rate."""
        return self.get('business_rules.tax_rate', 0.08)
    
    def get_cost_ratio(self) -> float:
        """Get configured cost ratio."""
        return self.get('business_rules.cost_ratio', 0.60)
    
    def reload_config(self, config_path: Optional[str] = None) -> None:
        """
        Reload configuration from file.
        
        Args:
            config_path: Path to configuration file
        """
        self._config.clear()
        self._load_config(config_path)


# Global configuration instance
config = ConfigManager()