"""
Configuration Manager for ETL System
Handles loading and accessing runtime configuration from YAML/JSON files
"""
import json
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from decimal import Decimal
from dataclasses import dataclass, field


@dataclass
class ETLConfig:
    """ETL Configuration Data Class"""
    
    # Processing Configuration
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    
    # Business Rules
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: Decimal = field(default_factory=lambda: Decimal('0.05'))
    discount_rate_tier2: Decimal = field(default_factory=lambda: Decimal('0.10'))
    tax_rate: Decimal = field(default_factory=lambda: Decimal('0.08'))
    cost_ratio: Decimal = field(default_factory=lambda: Decimal('0.60'))
    category_high_threshold: Decimal = field(default_factory=lambda: Decimal('2000.00'))
    category_medium_threshold: Decimal = field(default_factory=lambda: Decimal('500.00'))
    
    # Data Sources
    source_table: str = "zsales_raw"
    target_table: str = "zsales_analytics"
    log_table: str = "zetl_log"
    
    # File Paths
    data_path: str = "data/"
    log_path: str = "logs/"
    checkpoint_path: str = "checkpoints/"
    
    # Spark Configuration
    spark_app_name: str = "SalesETL"
    spark_master: str = "local[*]"
    spark_sql_shuffle_partitions: int = 200
    spark_executor_memory: str = "4g"
    spark_driver_memory: str = "2g"
    
    # Logging Configuration
    log_level: str = "INFO"
    enable_console_logging: bool = True
    enable_file_logging: bool = True
    
    # Test Mode
    test_mode: bool = False
    test_record_limit: int = 100


class ConfigManager:
    """
    Configuration Manager for ETL System
    Supports YAML and JSON configuration files
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize ConfigManager
        
        Args:
            config_path: Path to configuration file (YAML or JSON)
        """
        self._config: ETLConfig = ETLConfig()
        self._config_path: Optional[Path] = None
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> None:
        """
        Load configuration from file
        
        Args:
            config_path: Path to configuration file
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file format is invalid
        """
        path = Path(config_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        self._config_path = path
        
        # Load based on file extension
        if path.suffix.lower() in ['.yaml', '.yml']:
            config_dict = self._load_yaml(path)
        elif path.suffix.lower() == '.json':
            config_dict = self._load_json(path)
        else:
            raise ValueError(f"Unsupported config file format: {path.suffix}")
        
        # Update configuration
        self._update_config(config_dict)
    
    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """Load YAML configuration file"""
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    
    def _load_json(self, path: Path) -> Dict[str, Any]:
        """Load JSON configuration file"""
        with open(path, 'r') as f:
            return json.load(f)
    
    def _update_config(self, config_dict: Dict[str, Any]) -> None:
        """Update configuration from dictionary"""
        
        # Processing Configuration
        if 'processing' in config_dict:
            proc = config_dict['processing']
            self._config.batch_size = proc.get('batch_size', self._config.batch_size)
            self._config.commit_interval = proc.get('commit_interval', self._config.commit_interval)
            self._config.parallel_jobs = proc.get('parallel_jobs', self._config.parallel_jobs)
            self._config.retry_attempts = proc.get('retry_attempts', self._config.retry_attempts)
            self._config.timeout_seconds = proc.get('timeout_seconds', self._config.timeout_seconds)
        
        # Business Rules
        if 'business_rules' in config_dict:
            rules = config_dict['business_rules']
            self._config.discount_qty_tier1 = rules.get('discount_qty_tier1', self._config.discount_qty_tier1)
            self._config.discount_qty_tier2 = rules.get('discount_qty_tier2', self._config.discount_qty_tier2)
            self._config.discount_rate_tier1 = Decimal(str(rules.get('discount_rate_tier1', self._config.discount_rate_tier1)))
            self._config.discount_rate_tier2 = Decimal(str(rules.get('discount_rate_tier2', self._config.discount_rate_tier2)))
            self._config.tax_rate = Decimal(str(rules.get('tax_rate', self._config.tax_rate)))
            self._config.cost_ratio = Decimal(str(rules.get('cost_ratio', self._config.cost_ratio)))
            self._config.category_high_threshold = Decimal(str(rules.get('category_high_threshold', self._config.category_high_threshold)))
            self._config.category_medium_threshold = Decimal(str(rules.get('category_medium_threshold', self._config.category_medium_threshold)))
        
        # Data Sources
        if 'data_sources' in config_dict:
            ds = config_dict['data_sources']
            self._config.source_table = ds.get('source_table', self._config.source_table)
            self._config.target_table = ds.get('target_table', self._config.target_table)
            self._config.log_table = ds.get('log_table', self._config.log_table)
        
        # File Paths
        if 'paths' in config_dict:
            paths = config_dict['paths']
            self._config.data_path = paths.get('data_path', self._config.data_path)
            self._config.log_path = paths.get('log_path', self._config.log_path)
            self._config.checkpoint_path = paths.get('checkpoint_path', self._config.checkpoint_path)
        
        # Spark Configuration
        if 'spark' in config_dict:
            spark = config_dict['spark']
            self._config.spark_app_name = spark.get('app_name', self._config.spark_app_name)
            self._config.spark_master = spark.get('master', self._config.spark_master)
            self._config.spark_sql_shuffle_partitions = spark.get('sql_shuffle_partitions', self._config.spark_sql_shuffle_partitions)
            self._config.spark_executor_memory = spark.get('executor_memory', self._config.spark_executor_memory)
            self._config.spark_driver_memory = spark.get('driver_memory', self._config.spark_driver_memory)
        
        # Logging Configuration
        if 'logging' in config_dict:
            log = config_dict['logging']
            self._config.log_level = log.get('level', self._config.log_level)
            self._config.enable_console_logging = log.get('enable_console', self._config.enable_console_logging)
            self._config.enable_file_logging = log.get('enable_file', self._config.enable_file_logging)
        
        # Test Mode
        if 'test_mode' in config_dict:
            test = config_dict['test_mode']
            self._config.test_mode = test.get('enabled', self._config.test_mode)
            self._config.test_record_limit = test.get('record_limit', self._config.test_record_limit)
    
    def get_config(self) -> ETLConfig:
        """Get current configuration"""
        return self._config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        
        Args:
            key: Configuration key (supports dot notation, e.g., 'processing.batch_size')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = getattr(value, k)
            return value
        except AttributeError:
            return default
    
    def save_config(self, output_path: str) -> None:
        """
        Save current configuration to file
        
        Args:
            output_path: Path to save configuration
        """
        path = Path(output_path)
        
        # Convert config to dictionary
        config_dict = self._config_to_dict()
        
        # Save based on file extension
        if path.suffix.lower() in ['.yaml', '.yml']:
            self._save_yaml(config_dict, path)
        elif path.suffix.lower() == '.json':
            self._save_json(config_dict, path)
        else:
            raise ValueError(f"Unsupported config file format: {path.suffix}")
    
    def _config_to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'processing': {
                'batch_size': self._config.batch_size,
                'commit_interval': self._config.commit_interval,
                'parallel_jobs': self._config.parallel_jobs,
                'retry_attempts': self._config.retry_attempts,
                'timeout_seconds': self._config.timeout_seconds
            },
            'business_rules': {
                'discount_qty_tier1': self._config.discount_qty_tier1,
                'discount_qty_tier2': self._config.discount_qty_tier2,
                'discount_rate_tier1': float(self._config.discount_rate_tier1),
                'discount_rate_tier2': float(self._config.discount_rate_tier2),
                'tax_rate': float(self._config.tax_rate),
                'cost_ratio': float(self._config.cost_ratio),
                'category_high_threshold': float(self._config.category_high_threshold),
                'category_medium_threshold': float(self._config.category_medium_threshold)
            },
            'data_sources': {
                'source_table': self._config.source_table,
                'target_table': self._config.target_table,
                'log_table': self._config.log_table
            },
            'paths': {
                'data_path': self._config.data_path,
                'log_path': self._config.log_path,
                'checkpoint_path': self._config.checkpoint_path
            },
            'spark': {
                'app_name': self._config.spark_app_name,
                'master': self._config.spark_master,
                'sql_shuffle_partitions': self._config.spark_sql_shuffle_partitions,
                'executor_memory': self._config.spark_executor_memory,
                'driver_memory': self._config.spark_driver_memory
            },
            'logging': {
                'level': self._config.log_level,
                'enable_console': self._config.enable_console_logging,
                'enable_file': self._config.enable_file_logging
            },
            'test_mode': {
                'enabled': self._config.test_mode,
                'record_limit': self._config.test_record_limit
            }
        }
    
    def _save_yaml(self, config_dict: Dict[str, Any], path: Path) -> None:
        """Save configuration as YAML"""
        with open(path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
    
    def _save_json(self, config_dict: Dict[str, Any], path: Path) -> None:
        """Save configuration as JSON"""
        with open(path, 'w') as f:
            json.dump(config_dict, f, indent=2)
    
    def __repr__(self) -> str:
        """String representation"""
        return f"ConfigManager(config_path={self._config_path})"