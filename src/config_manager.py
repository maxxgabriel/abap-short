"""
Configuration Management Module for ETL System.

This module provides centralized configuration management for the ETL pipeline,
supporting YAML-based configuration files with environment-specific overrides.
"""

from typing import Any, Dict, Optional
import yaml
import os
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class ETLConfig:
    """ETL Configuration data class."""
    
    # Database configuration
    database: Dict[str, Any] = field(default_factory=dict)
    
    # Processing configuration
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    parallel_jobs: int = 1
    
    # Business rules
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: float = 0.05
    discount_rate_tier2: float = 0.10
    tax_rate: float = 0.08
    cost_ratio: float = 0.60
    category_high_threshold: float = 2000.00
    category_medium_threshold: float = 500.00
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: Optional[str] = None
    
    # ID prefixes
    prefix_etl_run: str = "ETL"
    prefix_log_id: str = "LOG"
    prefix_analytics_id: str = "ANL"
    
    # Status codes
    status_new: str = "N"
    status_processed: str = "P"
    status_error: str = "E"
    status_warning: str = "W"
    status_success: str = "S"
    status_info: str = "I"
    
    # Process steps
    step_init: str = "INIT"
    step_extract: str = "EXTRACT"
    step_transform: str = "TRANSFORM"
    step_load: str = "LOAD"
    step_validate: str = "VALIDATE"
    step_complete: str = "COMPLETE"
    step_error: str = "ERROR"
    
    # Categories
    category_high: str = "HIGH"
    category_medium: str = "MEDIUM"
    category_low: str = "LOW"


class ConfigManager:
    """
    Centralized configuration manager for the ETL system.
    
    Supports loading configuration from YAML files with environment variable
    overrides and provides a singleton instance for global access.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Optional[ETLConfig] = None
    
    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager."""
        if self._config is None:
            self._config = ETLConfig()
    
    def load_config(self, config_path: str, environment: str = "default") -> ETLConfig:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the configuration file
            environment: Environment name (default, dev, prod, etc.)
            
        Returns:
            ETLConfig: Loaded configuration object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Get environment-specific configuration
        env_config = config_data.get(environment, {})
        default_config = config_data.get('default', {})
        
        # Merge configurations (environment overrides default)
        merged_config = {**default_config, **env_config}
        
        # Update ETLConfig with loaded values
        self._config = self._build_config_object(merged_config)
        
        # Override with environment variables
        self._apply_env_overrides()
        
        return self._config
    
    def _build_config_object(self, config_dict: Dict[str, Any]) -> ETLConfig:
        """
        Build ETLConfig object from dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            ETLConfig: Configuration object
        """
        return ETLConfig(
            database=config_dict.get('database', {}),
            batch_size=config_dict.get('batch_size', 1000),
            commit_interval=config_dict.get('commit_interval', 500),
            retry_attempts=config_dict.get('retry_attempts', 3),
            timeout_seconds=config_dict.get('timeout_seconds', 3600),
            parallel_jobs=config_dict.get('parallel_jobs', 1),
            discount_qty_tier1=config_dict.get('discount_qty_tier1', 10),
            discount_qty_tier2=config_dict.get('discount_qty_tier2', 15),
            discount_rate_tier1=config_dict.get('discount_rate_tier1', 0.05),
            discount_rate_tier2=config_dict.get('discount_rate_tier2', 0.10),
            tax_rate=config_dict.get('tax_rate', 0.08),
            cost_ratio=config_dict.get('cost_ratio', 0.60),
            category_high_threshold=config_dict.get('category_high_threshold', 2000.00),
            category_medium_threshold=config_dict.get('category_medium_threshold', 500.00),
            log_level=config_dict.get('log_level', 'INFO'),
            log_format=config_dict.get('log_format', 
                                      '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            log_file=config_dict.get('log_file'),
            prefix_etl_run=config_dict.get('prefix_etl_run', 'ETL'),
            prefix_log_id=config_dict.get('prefix_log_id', 'LOG'),
            prefix_analytics_id=config_dict.get('prefix_analytics_id', 'ANL')
        )
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to configuration."""
        env_mappings = {
            'ETL_BATCH_SIZE': ('batch_size', int),
            'ETL_COMMIT_INTERVAL': ('commit_interval', int),
            'ETL_RETRY_ATTEMPTS': ('retry_attempts', int),
            'ETL_TIMEOUT_SECONDS': ('timeout_seconds', int),
            'ETL_LOG_LEVEL': ('log_level', str),
            'ETL_LOG_FILE': ('log_file', str),
            'ETL_TAX_RATE': ('tax_rate', float),
            'ETL_COST_RATIO': ('cost_ratio', float),
        }
        
        for env_var, (config_attr, type_func) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                setattr(self._config, config_attr, type_func(value))
    
    def get_config(self) -> ETLConfig:
        """
        Get current configuration.
        
        Returns:
            ETLConfig: Current configuration object
        """
        if self._config is None:
            self._config = ETLConfig()
        return self._config
    
    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration.
        
        Returns:
            Dict: Database configuration parameters
        """
        return self._config.database
    
    def get_spark_config(self) -> Dict[str, str]:
        """
        Get Spark-specific configuration.
        
        Returns:
            Dict: Spark configuration parameters
        """
        db_config = self._config.database
        return {
            'spark.app.name': db_config.get('app_name', 'SalesETL'),
            'spark.sql.warehouse.dir': db_config.get('warehouse_dir', '/tmp/spark-warehouse'),
            'spark.sql.shuffle.partitions': str(db_config.get('shuffle_partitions', 200)),
            'spark.executor.memory': db_config.get('executor_memory', '2g'),
            'spark.driver.memory': db_config.get('driver_memory', '2g'),
        }
    
    def validate_config(self) -> bool:
        """
        Validate configuration parameters.
        
        Returns:
            bool: True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        if self._config.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        
        if self._config.commit_interval <= 0:
            raise ValueError("commit_interval must be positive")
        
        if self._config.retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        
        if not (0 <= self._config.tax_rate <= 1):
            raise ValueError("tax_rate must be between 0 and 1")
        
        if not (0 <= self._config.cost_ratio <= 1):
            raise ValueError("cost_ratio must be between 0 and 1")
        
        return True


# Global configuration instance
config_manager = ConfigManager()


def get_config() -> ETLConfig:
    """
    Get global configuration instance.
    
    Returns:
        ETLConfig: Global configuration object
    """
    return config_manager.get_config()


def load_config(config_path: str, environment: str = "default") -> ETLConfig:
    """
    Load configuration from file.
    
    Args:
        config_path: Path to configuration file
        environment: Environment name
        
    Returns:
        ETLConfig: Loaded configuration
    """
    return config_manager.load_config(config_path, environment)