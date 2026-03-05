"""
Configuration Management
Loads and manages ETL configuration from YAML file
"""

import yaml
from pathlib import Path
from typing import Any, Optional
from decimal import Decimal


class Config:
    """Configuration manager for ETL process"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config_path = Path(config_path)
        self._config = self._load_config()
        
        # Extract configuration values
        self._extract_config_values()
    
    def _load_config(self) -> dict:
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _extract_config_values(self) -> None:
        """Extract configuration values into class attributes"""
        # Source configuration
        source = self._config.get('source', {})
        self.source_type = source.get('type', 'demo')
        self.source_table = source.get('table', 'raw_sales')
        self.source_path = source.get('path', 'data/input')
        
        # JDBC source configuration
        jdbc_source = source.get('jdbc', {})
        self.jdbc_url = jdbc_source.get('url', '')
        self.jdbc_user = jdbc_source.get('user', '')
        self.jdbc_password = jdbc_source.get('password', '')
        self.jdbc_driver = jdbc_source.get('driver', 'org.postgresql.Driver')
        
        # Target configuration
        target = self._config.get('target', {})
        self.target_type = target.get('type', 'parquet')
        self.target_table = target.get('table', 'analytics_sales')
        self.target_path = target.get('path', 'data/output')
        self.write_mode = target.get('write_mode', 'append')
        
        # JDBC target configuration
        jdbc_target = target.get('jdbc', {})
        self.target_jdbc_url = jdbc_target.get('url', '')
        self.target_jdbc_user = jdbc_target.get('user', '')
        self.target_jdbc_password = jdbc_target.get('password', '')
        self.target_jdbc_driver = jdbc_target.get('driver', 'org.postgresql.Driver')
        
        # Business rules
        business_rules = self._config.get('business_rules', {})
        self.discount_qty_tier1 = business_rules.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = business_rules.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = Decimal(str(business_rules.get('discount_rate_tier1', 0.05)))
        self.discount_rate_tier2 = Decimal(str(business_rules.get('discount_rate_tier2', 0.10)))
        self.tax_rate = Decimal(str(business_rules.get('tax_rate', 0.08)))
        self.cost_ratio = Decimal(str(business_rules.get('cost_ratio', 0.60)))
        self.category_high_threshold = Decimal(str(business_rules.get('category_high_threshold', 2000.00)))
        self.category_medium_threshold = Decimal(str(business_rules.get('category_medium_threshold', 500.00)))
        
        # ETL configuration
        etl_config = self._config.get('etl_config', {})
        self.batch_size = etl_config.get('batch_size', 1000)
        self.commit_interval = etl_config.get('commit_interval', 500)
        self.parallel_jobs = etl_config.get('parallel_jobs', 4)
        self.retry_attempts = etl_config.get('retry_attempts', 3)
        self.timeout_seconds = etl_config.get('timeout_seconds', 3600)
        self.validate_before_load = etl_config.get('validate_before_load', True)
        
        # Logging configuration
        logging_config = self._config.get('logging', {})
        self.log_level = logging_config.get('level', 'INFO')
        self.log_path = logging_config.get('path', 'logs')
        self.enable_db_logging = logging_config.get('enable_db_logging', False)
        
        # Spark configuration
        self.spark_config = self._config.get('spark', {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)