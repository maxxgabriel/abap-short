"""
Configuration management module for ETL framework.
Provides centralized configuration loading and validation.
"""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import logging


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass


class ConfigManager:
    """
    Centralized configuration manager for ETL operations.
    Loads and validates configuration from YAML files.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file. If None, uses default path.
        """
        self.logger = logging.getLogger(__name__)
        
        if config_path is None:
            # Default to config.yaml in project root
            config_path = Path(__file__).parent.parent / "config.yaml"
        
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()
        self._validate_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        try:
            if not self.config_path.exists():
                raise ConfigurationError(
                    f"Configuration file not found: {self.config_path}"
                )
            
            with open(self.config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            
            self.logger.info(f"Configuration loaded from {self.config_path}")
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def _validate_config(self) -> None:
        """Validate required configuration sections exist."""
        required_sections = ['spark', 'database', 'etl', 'logging']
        
        for section in required_sections:
            if section not in self._config:
                raise ConfigurationError(
                    f"Required configuration section missing: {section}"
                )
        
        # Validate spark configuration
        spark_config = self._config['spark']
        if 'app_name' not in spark_config:
            raise ConfigurationError("spark.app_name is required")
        
        # Validate database configuration
        db_config = self._config['database']
        required_db_fields = ['host', 'port', 'database']
        for field in required_db_fields:
            if field not in db_config:
                raise ConfigurationError(f"database.{field} is required")
        
        # Validate ETL configuration
        etl_config = self._config['etl']
        if 'batch_size' not in etl_config or etl_config['batch_size'] <= 0:
            raise ConfigurationError("etl.batch_size must be a positive integer")
        
        self.logger.info("Configuration validation successful")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key using dot notation.
        
        Args:
            key: Configuration key (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
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
    
    def get_business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration."""
        return self._config.get('etl', {}).get('business_rules', {})
    
    def get_batch_size(self) -> int:
        """Get configured batch size."""
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
    
    def get_connection_string(self) -> str:
        """
        Build database connection string from configuration.
        
        Returns:
            JDBC connection string
        """
        db_config = self.get_database_config()
        
        host = db_config.get('host')
        port = db_config.get('port')
        database = db_config.get('database')
        
        return f"jdbc:postgresql://{host}:{port}/{database}"
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self.logger.info("Reloading configuration")
        self._load_config()
        self._validate_config()


# Singleton instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager(config_path: Optional[str] = None) -> ConfigManager:
    """
    Get or create singleton ConfigManager instance.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        ConfigManager instance
    """
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
    
    return _config_manager