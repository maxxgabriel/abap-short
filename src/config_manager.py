"""
Configuration Manager
Centralized configuration management for ETL framework
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration for ETL framework"""
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        """Singleton pattern to ensure only one config instance"""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager"""
        if not self._config:
            self.load_config()
    
    def load_config(self, config_path: Optional[str] = None) -> None:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to config file. If None, uses default location
        """
        if config_path is None:
            # Try multiple locations
            possible_paths = [
                'config.yaml',
                'config/config.yaml',
                '../config.yaml',
                os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    break
            
            if config_path is None:
                raise FileNotFoundError("Configuration file not found in expected locations")
        
        try:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            
            # Resolve environment variables
            self._resolve_env_vars(self._config)
            
            logger.info(f"Configuration loaded from: {config_path}")
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def _resolve_env_vars(self, config: Dict[str, Any]) -> None:
        """
        Recursively resolve environment variables in config
        Format: ${VAR_NAME:default_value}
        """
        import re
        env_pattern = re.compile(r'\$\{([^:}]+)(?::([^}]+))?\}')
        
        for key, value in config.items():
            if isinstance(value, dict):
                self._resolve_env_vars(value)
            elif isinstance(value, str):
                match = env_pattern.search(value)
                if match:
                    env_var = match.group(1)
                    default = match.group(2) or ''
                    config[key] = os.getenv(env_var, default)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Args:
            key_path: Dot-separated path (e.g., 'database.source.url')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
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
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.get('logging', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration"""
        return self.get('etl', {})
    
    def get_business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration"""
        return self.get('business_rules', {})
    
    def get_validation_rules(self) -> Dict[str, Any]:
        """Get validation rules"""
        return self.get('validation', {})
    
    def get_app_name(self) -> str:
        """Get application name"""
        return self.get('application.name', 'ETL Application')
    
    def get_app_version(self) -> str:
        """Get application version"""
        return self.get('application.version', '1.0.0')
    
    def get_environment(self) -> str:
        """Get environment (development, production, etc.)"""
        return self.get('application.environment', 'development')
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.get_environment().lower() == 'production'
    
    def reload(self, config_path: Optional[str] = None) -> None:
        """Reload configuration"""
        self._config = {}
        self.load_config(config_path)
    
    def to_dict(self) -> Dict[str, Any]:
        """Get full configuration as dictionary"""
        return self._config.copy()


# Global config instance
config = ConfigManager()