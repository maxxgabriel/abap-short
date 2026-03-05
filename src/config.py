"""
Configuration Loading Module

This module handles loading and validating ETL configuration
from YAML files and environment variables.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dataclasses import dataclass, field, asdict

from src.constants import ETLConstants


@dataclass
class ETLConfig:
    """ETL configuration data class"""
    
    # Processing configuration
    batch_size: int = ETLConstants.DEFAULTS.BATCH_SIZE
    commit_interval: int = ETLConstants.DEFAULTS.COMMIT_INTERVAL
    retry_attempts: int = ETLConstants.DEFAULTS.RETRY_ATTEMPTS
    timeout_seconds: int = ETLConstants.DEFAULTS.TIMEOUT_SECONDS
    
    # Spark configuration
    spark_app_name: str = "SalesETL"
    spark_master: str = "local[*]"
    spark_config: Dict[str, str] = field(default_factory=lambda: {
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    })
    
    # Data paths
    input_path: Optional[str] = None
    output_path: Optional[str] = None
    log_path: Optional[str] = None
    
    # Database configuration (if applicable)
    db_url: Optional[str] = None
    db_table_raw: str = "zsales_raw"
    db_table_analytics: str = "zsales_analytics"
    db_table_log: str = "zetl_log"
    
    # Business rules override
    rules_override: Dict[str, Any] = field(default_factory=dict)
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        return asdict(self)
    
    def validate(self) -> bool:
        """Validate configuration values"""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.commit_interval <= 0:
            raise ValueError("commit_interval must be positive")
        if self.retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            raise ValueError(f"Invalid log_level: {self.log_level}")
        return True


class ConfigLoader:
    """Configuration loader with multiple source support"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize config loader
        
        Args:
            config_path: Path to YAML config file (optional)
        """
        self.config_path = config_path or self._find_default_config()
    
    @staticmethod
    def _find_default_config() -> Optional[str]:
        """Find default config file in standard locations"""
        search_paths = [
            Path("config.yaml"),
            Path("config/config.yaml"),
            Path("conf/config.yaml"),
            Path.home() / ".etl" / "config.yaml"
        ]
        
        for path in search_paths:
            if path.exists():
                return str(path)
        return None
    
    def load(self) -> ETLConfig:
        """
        Load configuration from file and environment
        
        Returns:
            ETLConfig instance with loaded configuration
        """
        config_dict = self._load_yaml_config()
        config_dict = self._merge_env_vars(config_dict)
        
        config = ETLConfig(**config_dict)
        config.validate()
        
        return config
    
    def _load_yaml_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not self.config_path or not Path(self.config_path).exists():
            return {}
        
        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f) or {}
            return config_data
        except Exception as e:
            raise ValueError(f"Failed to load config from {self.config_path}: {e}")
    
    def _merge_env_vars(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Merge environment variables into configuration"""
        env_mapping = {
            'ETL_BATCH_SIZE': ('batch_size', int),
            'ETL_COMMIT_INTERVAL': ('commit_interval', int),
            'ETL_RETRY_ATTEMPTS': ('retry_attempts', int),
            'ETL_TIMEOUT_SECONDS': ('timeout_seconds', int),
            'ETL_SPARK_MASTER': ('spark_master', str),
            'ETL_INPUT_PATH': ('input_path', str),
            'ETL_OUTPUT_PATH': ('output_path', str),
            'ETL_LOG_PATH': ('log_path', str),
            'ETL_DB_URL': ('db_url', str),
            'ETL_LOG_LEVEL': ('log_level', str),
        }
        
        for env_var, (config_key, type_func) in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    config_dict[config_key] = type_func(value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid value for {env_var}: {value} - {e}")
        
        return config_dict
    
    @staticmethod
    def create_default_config(output_path: str = "config.yaml") -> None:
        """
        Create a default configuration file
        
        Args:
            output_path: Path where to save the config file
        """
        default_config = {
            'batch_size': ETLConstants.DEFAULTS.BATCH_SIZE,
            'commit_interval': ETLConstants.DEFAULTS.COMMIT_INTERVAL,
            'retry_attempts': ETLConstants.DEFAULTS.RETRY_ATTEMPTS,
            'timeout_seconds': ETLConstants.DEFAULTS.TIMEOUT_SECONDS,
            'spark_app_name': 'SalesETL',
            'spark_master': 'local[*]',
            'spark_config': {
                'spark.sql.shuffle.partitions': '200',
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true'
            },
            'input_path': '/data/input',
            'output_path': '/data/output',
            'log_path': '/data/logs',
            'db_table_raw': 'zsales_raw',
            'db_table_analytics': 'zsales_analytics',
            'db_table_log': 'zetl_log',
            'log_level': 'INFO',
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
        
        with open(output_path, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, sort_keys=False)