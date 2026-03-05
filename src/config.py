"""
Configuration Management Module
Handles loading and managing ETL configuration from YAML files
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
import yaml


@dataclass
class BusinessRules:
    """Business rules configuration"""
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: float = 0.05
    discount_rate_tier2: float = 0.10
    tax_rate: float = 0.08
    cost_ratio: float = 0.60
    category_high_threshold: float = 2000.00
    category_medium_threshold: float = 500.00


@dataclass
class ETLConfig:
    """ETL process configuration"""
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    parallel_jobs: int = 4


@dataclass
class SparkConfig:
    """Spark session configuration"""
    app_name: str = "SalesETL"
    master: str = "local[*]"
    executor_memory: str = "2g"
    driver_memory: str = "1g"
    shuffle_partitions: int = 200
    dynamic_allocation_enabled: bool = True
    log_level: str = "WARN"


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    jdbc_url: str = ""
    driver: str = "org.postgresql.Driver"
    user: str = ""
    password: str = ""
    source_table: str = "zsales_raw"
    target_table: str = "zsales_analytics"
    log_table: str = "zetl_log"


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: str = "logs/etl.log"
    max_bytes: int = 10485760  # 10MB
    backup_count: int = 5
    console_output: bool = True


@dataclass
class Configuration:
    """Master configuration container"""
    business_rules: BusinessRules = field(default_factory=BusinessRules)
    etl: ETLConfig = field(default_factory=ETLConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


class ConfigurationManager:
    """Centralized configuration management"""
    
    _instance: Optional['ConfigurationManager'] = None
    _config: Optional[Configuration] = None
    
    def __new__(cls):
        """Singleton pattern implementation"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager"""
        if self._config is None:
            self._config = Configuration()
    
    @classmethod
    def load_from_file(cls, config_path: str) -> 'ConfigurationManager':
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            ConfigurationManager instance
        """
        instance = cls()
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        instance._load_config_data(config_data)
        return instance
    
    def _load_config_data(self, config_data: Dict[str, Any]) -> None:
        """Load configuration data into dataclass structures"""
        if 'business_rules' in config_data:
            self._config.business_rules = BusinessRules(**config_data['business_rules'])
        
        if 'etl' in config_data:
            self._config.etl = ETLConfig(**config_data['etl'])
        
        if 'spark' in config_data:
            self._config.spark = SparkConfig(**config_data['spark'])
        
        if 'database' in config_data:
            self._config.database = DatabaseConfig(**config_data['database'])
        
        if 'logging' in config_data:
            self._config.logging = LoggingConfig(**config_data['logging'])
    
    @classmethod
    def load_from_env(cls) -> 'ConfigurationManager':
        """
        Load configuration from environment variables
        
        Returns:
            ConfigurationManager instance
        """
        instance = cls()
        
        # Database configuration from environment
        if os.getenv('DB_JDBC_URL'):
            instance._config.database.jdbc_url = os.getenv('DB_JDBC_URL')
        if os.getenv('DB_USER'):
            instance._config.database.user = os.getenv('DB_USER')
        if os.getenv('DB_PASSWORD'):
            instance._config.database.password = os.getenv('DB_PASSWORD')
        
        # Spark configuration from environment
        if os.getenv('SPARK_MASTER'):
            instance._config.spark.master = os.getenv('SPARK_MASTER')
        if os.getenv('SPARK_EXECUTOR_MEMORY'):
            instance._config.spark.executor_memory = os.getenv('SPARK_EXECUTOR_MEMORY')
        
        # ETL configuration from environment
        if os.getenv('ETL_BATCH_SIZE'):
            instance._config.etl.batch_size = int(os.getenv('ETL_BATCH_SIZE'))
        
        return instance
    
    def get_config(self) -> Configuration:
        """Get configuration object"""
        return self._config
    
    def get_business_rules(self) -> BusinessRules:
        """Get business rules configuration"""
        return self._config.business_rules
    
    def get_etl_config(self) -> ETLConfig:
        """Get ETL configuration"""
        return self._config.etl
    
    def get_spark_config(self) -> SparkConfig:
        """Get Spark configuration"""
        return self._config.spark
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration"""
        return self._config.database
    
    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration"""
        return self._config.logging
    
    def update_config(self, section: str, **kwargs) -> None:
        """
        Update specific configuration section
        
        Args:
            section: Configuration section name (business_rules, etl, spark, database, logging)
            **kwargs: Configuration parameters to update
        """
        if section == 'business_rules':
            for key, value in kwargs.items():
                if hasattr(self._config.business_rules, key):
                    setattr(self._config.business_rules, key, value)
        elif section == 'etl':
            for key, value in kwargs.items():
                if hasattr(self._config.etl, key):
                    setattr(self._config.etl, key, value)
        elif section == 'spark':
            for key, value in kwargs.items():
                if hasattr(self._config.spark, key):
                    setattr(self._config.spark, key, value)
        elif section == 'database':
            for key, value in kwargs.items():
                if hasattr(self._config.database, key):
                    setattr(self._config.database, key, value)
        elif section == 'logging':
            for key, value in kwargs.items():
                if hasattr(self._config.logging, key):
                    setattr(self._config.logging, key, value)
        else:
            raise ValueError(f"Unknown configuration section: {section}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'business_rules': self._config.business_rules.__dict__,
            'etl': self._config.etl.__dict__,
            'spark': self._config.spark.__dict__,
            'database': self._config.database.__dict__,
            'logging': self._config.logging.__dict__
        }