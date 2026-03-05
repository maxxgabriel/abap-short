"""
Configuration Loader Module

Handles loading and validation of ETL configuration from YAML files.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass

from src.exceptions import ConfigurationError
from src.constants import ETLConstants


@dataclass
class ETLConfig:
    """ETL configuration data structure"""
    batch_size: int
    commit_interval: int
    retry_attempts: int
    timeout_seconds: int
    parallel_jobs: int = 1
    source_path: Optional[str] = None
    target_path: Optional[str] = None
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    def validate(self) -> None:
        """Validate configuration values"""
        if self.batch_size <= 0:
            raise ConfigurationError("batch_size must be positive", config_key="batch_size")
        
        if self.commit_interval <= 0:
            raise ConfigurationError("commit_interval must be positive", config_key="commit_interval")
        
        if self.retry_attempts < 0:
            raise ConfigurationError("retry_attempts must be non-negative", config_key="retry_attempts")
        
        if self.timeout_seconds <= 0:
            raise ConfigurationError("timeout_seconds must be positive", config_key="timeout_seconds")
        
        if self.parallel_jobs <= 0:
            raise ConfigurationError("parallel_jobs must be positive", config_key="parallel_jobs")
        
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if self.log_level.upper() not in valid_log_levels:
            raise ConfigurationError(
                f"log_level must be one of {valid_log_levels}",
                config_key="log_level"
            )


class ConfigLoader:
    """Configuration loader and manager"""
    
    @staticmethod
    def load_config(config_path: str) -> ETLConfig:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            ETLConfig object with loaded configuration
            
        Raises:
            ConfigurationError: If config file is invalid or missing
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Failed to parse configuration file: {config_path}",
                original_exception=e
            )
        except Exception as e:
            raise ConfigurationError(
                f"Failed to read configuration file: {config_path}",
                original_exception=e
            )
        
        # Build config object with defaults
        config = ETLConfig(
            batch_size=config_data.get('batch_size', ETLConstants.DEFAULTS.BATCH_SIZE),
            commit_interval=config_data.get('commit_interval', ETLConstants.DEFAULTS.COMMIT_INTERVAL),
            retry_attempts=config_data.get('retry_attempts', ETLConstants.DEFAULTS.RETRY_ATTEMPTS),
            timeout_seconds=config_data.get('timeout_seconds', ETLConstants.DEFAULTS.TIMEOUT_SECONDS),
            parallel_jobs=config_data.get('parallel_jobs', 1),
            source_path=config_data.get('source_path'),
            target_path=config_data.get('target_path'),
            log_level=config_data.get('log_level', 'INFO'),
            log_file=config_data.get('log_file')
        )
        
        # Validate configuration
        config.validate()
        
        return config
    
    @staticmethod
    def create_default_config(output_path: str) -> None:
        """
        Create a default configuration file
        
        Args:
            output_path: Path where config file should be created
        """
        default_config = {
            'batch_size': ETLConstants.DEFAULTS.BATCH_SIZE,
            'commit_interval': ETLConstants.DEFAULTS.COMMIT_INTERVAL,
            'retry_attempts': ETLConstants.DEFAULTS.RETRY_ATTEMPTS,
            'timeout_seconds': ETLConstants.DEFAULTS.TIMEOUT_SECONDS,
            'parallel_jobs': 1,
            'log_level': 'INFO',
            'log_file': 'logs/etl.log',
            'source_path': 'data/input',
            'target_path': 'data/output',
            'business_rules': {
                'discount_qty_tier1': ETLConstants.RULES.DISCOUNT_QTY_TIER1,
                'discount_qty_tier2': ETLConstants.RULES.DISCOUNT_QTY_TIER2,
                'discount_rate_tier1': ETLConstants.RULES.DISCOUNT_RATE_TIER1,
                'discount_rate_tier2': ETLConstants.RULES.DISCOUNT_RATE_TIER2,
                'tax_rate': ETLConstants.RULES.TAX_RATE,
                'cost_ratio': ETLConstants.RULES.COST_RATIO,
                'category_high_threshold': ETLConstants.RULES.CATEGORY_HIGH_THRESHOLD,
                'category_medium_threshold': ETLConstants.RULES.CATEGORY_MEDIUM_THRESHOLD
            }
        }
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, sort_keys=False)