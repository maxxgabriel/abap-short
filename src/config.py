"""
Configuration Module - Sales ETL System
Handles ETL configuration loading and access
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional
import yaml
import os


@dataclass
class ETLConfig:
    """ETL configuration container"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config_data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            # Return default configuration if file doesn't exist
            return self._get_default_config()
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports dot notation)
        
        Args:
            key: Configuration key (e.g., 'business_rules.tax_rate')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config_data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "business_rules": {
                "discount_qty_tier1": 10,
                "discount_qty_tier2": 15,
                "discount_rate_tier1": 0.05,
                "discount_rate_tier2": 0.10,
                "tax_rate": 0.08,
                "cost_ratio": 0.60,
                "category_high_threshold": 2000.00,
                "category_medium_threshold": 500.00
            },
            "etl_config": {
                "batch_size": 1000,
                "commit_interval": 500,
                "retry_attempts": 3,
                "timeout_seconds": 3600
            },
            "id_prefixes": {
                "etl_run": "ETL",
                "log": "LOG",
                "analytics": "ANL"
            },
            "tables": {
                "source": "zsales_raw",
                "target": "zsales_analytics",
                "log": "zetl_log"
            },
            "spark": {
                "app_name": "Sales ETL Process",
                "master": "local[*]",
                "executor_memory": "2g",
                "driver_memory": "1g"
            }
        }