"""
Configuration Manager Module
Centralized configuration management for ETL system.
"""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
import logging


class ConfigurationManager:
    """Manages configuration loading and access for ETL processes."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file (defaults to config.yaml)
        """
        self._logger = logging.getLogger(__name__)
        
        if config_path is None:
            # Look for config.yaml in project root or config directory
            possible_paths = [
                Path("config.yaml"),
                Path("config/config.yaml"),
                Path("../config.yaml")
            ]
            
            for path in possible_paths:
                if path.exists():
                    config_path = str(path)
                    break
            
            if config_path is None:
                raise FileNotFoundError("Configuration file not found")
        
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        try:
            self._logger.info(f"Loading configuration from {self.config_path}")
            
            with open(self.config_path, 'r') as file:
                self._config = yaml.safe_load(file)
            
            # Apply environment variable overrides
            self._apply_env_overrides()
            
            self._logger.info("Configuration loaded successfully")
            
        except FileNotFoundError:
            self._logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            self._logger.error(f"Error parsing YAML configuration: {e}")
            raise
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to configuration."""
        # Database connection overrides
        if os.getenv("DB_HOST"):
            self._config.setdefault("database", {})["host"] = os.getenv("DB_HOST")
        if os.getenv("DB_PORT"):
            self._config.setdefault("database", {})["port"] = int(os.getenv("DB_PORT"))
        if os.getenv("DB_NAME"):
            self._config.setdefault("database", {})["database"] = os.getenv("DB_NAME")
        if os.getenv("DB_USER"):
            self._config.setdefault("database", {})["user"] = os.getenv("DB_USER")
        if os.getenv("DB_PASSWORD"):
            self._config.setdefault("database", {})["password"] = os.getenv("DB_PASSWORD")
        
        # ETL configuration overrides
        if os.getenv("BATCH_SIZE"):
            self._config.setdefault("etl", {})["batch_size"] = int(os.getenv("BATCH_SIZE"))
        if os.getenv("PARALLEL_JOBS"):
            self._config.setdefault("etl", {})["parallel_jobs"] = int(os.getenv("PARALLEL_JOBS"))
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports dot notation).
        
        Args:
            key: Configuration key (e.g., 'database.host')
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
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self._config.get("database", {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration."""
        return self._config.get("etl", {})
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration."""
        return self._config.get("spark", {})
    
    def get_business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration."""
        return self._config.get("business_rules", {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self._config.get("logging", {})
    
    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary."""
        return self._config.copy()
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self._logger.info("Reloading configuration")
        self._load_config()


# Global configuration instance
_config_instance: Optional[ConfigurationManager] = None


def get_config(config_path: Optional[str] = None) -> ConfigurationManager:
    """
    Get global configuration manager instance.
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        ConfigurationManager instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigurationManager(config_path)
    
    return _config_instance