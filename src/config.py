"""Configuration management for ETL process."""
import os
import yaml
from typing import Dict, Any
from pathlib import Path


class ETLConfig:
    """Manages ETL configuration from YAML file and environment variables."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self._substitute_env_vars()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_file = Path(self.config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def _substitute_env_vars(self):
        """Substitute environment variables in configuration values."""
        def substitute_dict(d: Dict[str, Any]) -> Dict[str, Any]:
            for key, value in d.items():
                if isinstance(value, dict):
                    d[key] = substitute_dict(value)
                elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    env_var = value[2:-1]
                    d[key] = os.getenv(env_var, value)
            return d
        
        self.config = substitute_dict(self.config)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to config value (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    @property
    def spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration."""
        return self.config.get('spark', {})
    
    @property
    def database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.config.get('database', {})
    
    @property
    def etl_config(self) -> Dict[str, Any]:
        """Get ETL-specific configuration."""
        return self.config.get('etl', {})
    
    @property
    def business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration."""
        return self.config.get('business_rules', {})