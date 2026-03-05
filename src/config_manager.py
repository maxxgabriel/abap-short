"""
Configuration Manager for ETL Framework
Provides centralized access to configuration with environment variable support
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
import logging


class ConfigurationError(Exception):
    """Raised when configuration issues occur"""
    pass


class ConfigManager:
    """
    Centralized configuration management for ETL operations.
    Supports YAML config files with environment variable substitution.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[Dict[str, Any]] = None
    
    def __new__(cls):
        """Singleton pattern to ensure single config instance"""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager"""
        if self._config is None:
            self._config = {}
            self._logger = logging.getLogger(__name__)
    
    def load_config(self, config_path: str = "config.yaml") -> None:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
            
        Raises:
            ConfigurationError: If config file not found or invalid
        """
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                raise ConfigurationError(f"Configuration file not found: {config_path}")
            
            with open(config_file, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            # Substitute environment variables
            self._config = self._substitute_env_vars(raw_config)
            
            self._logger.info(f"Configuration loaded successfully from {config_path}")
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in config
        Format: ${VAR_NAME} or ${VAR_NAME:default_value}
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Handle ${VAR_NAME} or ${VAR_NAME:default}
            if config.startswith("${") and config.endswith("}"):
                var_expr = config[2:-1]
                if ":" in var_expr:
                    var_name, default_value = var_expr.split(":", 1)
                    return os.getenv(var_name, default_value)
                else:
                    value = os.getenv(var_expr)
                    if value is None:
                        raise ConfigurationError(
                            f"Environment variable {var_expr} not set and no default provided"
                        )
                    return value
        return config
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Args:
            key_path: Path to config value (e.g., 'database.source.jdbc_url')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        if not self._config:
            raise ConfigurationError("Configuration not loaded. Call load_config() first.")
        
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration as dictionary"""
        return self.get('spark.config', {})
    
    def get_database_config(self, db_type: str = 'source') -> Dict[str, Any]:
        """
        Get database configuration
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            Database configuration dictionary
        """
        return self.get(f'database.{db_type}', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL processing configuration"""
        return self.get('etl', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.get('logging', {})
    
    def get_business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration"""
        return self.get('etl.business_rules', {})
    
    def validate_config(self) -> bool:
        """
        Validate required configuration keys
        
        Returns:
            True if valid, raises ConfigurationError otherwise
        """
        required_keys = [
            'spark.app_name',
            'database.source.jdbc_url',
            'database.target.jdbc_url',
            'etl.processing.batch_size'
        ]
        
        for key in required_keys:
            if self.get(key) is None:
                raise ConfigurationError(f"Required configuration key missing: {key}")
        
        return True
    
    def reload_config(self, config_path: str = "config.yaml") -> None:
        """Reload configuration from file"""
        self._config = None
        self.load_config(config_path)
    
    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary"""
        if not self._config:
            raise ConfigurationError("Configuration not loaded")
        return self._config.copy()


# Singleton instance accessor
def get_config() -> ConfigManager:
    """Get singleton ConfigManager instance"""
    return ConfigManager()