"""
Configuration management module for the ETL framework.
Provides centralized configuration loading and validation.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import yaml
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: str = "postgresql"
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600


@dataclass
class SparkConfig:
    """Spark session configuration."""
    app_name: str = "Sales ETL System"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    executor_cores: int = 2
    shuffle_partitions: int = 200
    dynamic_allocation: bool = True
    additional_conf: Dict[str, str] = field(default_factory=dict)


@dataclass
class ETLConfig:
    """ETL process configuration."""
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    parallel_jobs: int = 4
    checkpoint_enabled: bool = True
    checkpoint_location: str = "/tmp/etl_checkpoint"


@dataclass
class BusinessRulesConfig:
    """Business rules configuration."""
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: float = 0.05
    discount_rate_tier2: float = 0.10
    tax_rate: float = 0.08
    cost_ratio: float = 0.60
    category_high_threshold: float = 2000.00
    category_medium_threshold: float = 500.00


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "logs"
    log_file: str = "etl_process.log"
    max_bytes: int = 10485760  # 10MB
    backup_count: int = 5
    console_output: bool = True


class ConfigurationManager:
    """
    Centralized configuration manager for the ETL system.
    Loads and validates configuration from YAML files.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.

        Args:
            config_path: Path to configuration file. Defaults to config.yaml
        """
        self.config_path = Path(config_path or "config.yaml")
        self._config_data: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}"
            )

        with open(self.config_path, 'r') as f:
            self._config_data = yaml.safe_load(f) or {}

    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration."""
        db_conf = self._config_data.get('database', {})
        return DatabaseConfig(**db_conf)

    def get_spark_config(self) -> SparkConfig:
        """Get Spark configuration."""
        spark_conf = self._config_data.get('spark', {})
        return SparkConfig(**spark_conf)

    def get_etl_config(self) -> ETLConfig:
        """Get ETL process configuration."""
        etl_conf = self._config_data.get('etl', {})
        return ETLConfig(**etl_conf)

    def get_business_rules_config(self) -> BusinessRulesConfig:
        """Get business rules configuration."""
        rules_conf = self._config_data.get('business_rules', {})
        return BusinessRulesConfig(**rules_conf)

    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration."""
        log_conf = self._config_data.get('logging', {})
        return LoggingConfig(**log_conf)

    def get_raw_config(self) -> Dict[str, Any]:
        """Get raw configuration dictionary."""
        return self._config_data.copy()

    def get_value(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated key path.

        Args:
            key_path: Dot-separated key path (e.g., 'database.host')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self._config_data

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default

        return value

    def validate_config(self) -> bool:
        """
        Validate configuration completeness.

        Returns:
            True if valid, raises ValueError otherwise
        """
        required_sections = ['database', 'spark', 'etl', 'business_rules', 'logging']

        for section in required_sections:
            if section not in self._config_data:
                raise ValueError(f"Missing required configuration section: {section}")

        # Validate database config
        db_conf = self._config_data['database']
        required_db_fields = ['host', 'port', 'database', 'username', 'password']
        for field in required_db_fields:
            if field not in db_conf:
                raise ValueError(f"Missing required database field: {field}")

        return True


# Singleton instance
_config_manager: Optional[ConfigurationManager] = None


def get_config_manager(config_path: Optional[str] = None) -> ConfigurationManager:
    """
    Get singleton configuration manager instance.

    Args:
        config_path: Path to configuration file

    Returns:
        ConfigurationManager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigurationManager(config_path)
    return _config_manager