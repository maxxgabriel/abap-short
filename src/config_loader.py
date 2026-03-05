"""
Configuration loader for ETL system.
Replaces ABAP ZCL_ETL_CONSTANTS class functionality.
"""

import yaml
from typing import Dict, Any
from pathlib import Path


class ETLConfig:
    """
    Configuration manager for ETL system.
    Migrated from ABAP ZCL_ETL_CONSTANTS.
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path: Path to configuration YAML file
        """
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self._config: Dict[str, Any] = yaml.safe_load(f)
    
    # Status codes (replaces gc_status)
    @property
    def status_new(self) -> str:
        return self._config['status_codes']['new']
    
    @property
    def status_processed(self) -> str:
        return self._config['status_codes']['processed']
    
    @property
    def status_error(self) -> str:
        return self._config['status_codes']['error']
    
    @property
    def status_warning(self) -> str:
        return self._config['status_codes']['warning']
    
    @property
    def status_success(self) -> str:
        return self._config['status_codes']['success']
    
    @property
    def status_info(self) -> str:
        return self._config['status_codes']['info']
    
    # Process steps (replaces gc_step)
    @property
    def step_init(self) -> str:
        return self._config['process_steps']['init']
    
    @property
    def step_extract(self) -> str:
        return self._config['process_steps']['extract']
    
    @property
    def step_transform(self) -> str:
        return self._config['process_steps']['transform']
    
    @property
    def step_load(self) -> str:
        return self._config['process_steps']['load']
    
    @property
    def step_validate(self) -> str:
        return self._config['process_steps']['validate']
    
    @property
    def step_complete(self) -> str:
        return self._config['process_steps']['complete']
    
    @property
    def step_error(self) -> str:
        return self._config['process_steps']['error']
    
    # Categories (replaces gc_category)
    @property
    def category_high(self) -> str:
        return self._config['categories']['high']
    
    @property
    def category_medium(self) -> str:
        return self._config['categories']['medium']
    
    @property
    def category_low(self) -> str:
        return self._config['categories']['low']
    
    # Business rules - Discount
    @property
    def discount_qty_tier1(self) -> int:
        return self._config['business_rules']['discount']['quantity_tier1']
    
    @property
    def discount_qty_tier2(self) -> int:
        return self._config['business_rules']['discount']['quantity_tier2']
    
    @property
    def discount_rate_tier1(self) -> float:
        return self._config['business_rules']['discount']['rate_tier1']
    
    @property
    def discount_rate_tier2(self) -> float:
        return self._config['business_rules']['discount']['rate_tier2']
    
    # Business rules - Tax
    @property
    def tax_rate(self) -> float:
        return self._config['business_rules']['tax_rate']
    
    # Business rules - Cost
    @property
    def cost_ratio(self) -> float:
        return self._config['business_rules']['cost_ratio']
    
    # Business rules - Category thresholds
    @property
    def category_high_threshold(self) -> float:
        return self._config['business_rules']['category']['high_threshold']
    
    @property
    def category_medium_threshold(self) -> float:
        return self._config['business_rules']['category']['medium_threshold']
    
    # ETL configuration
    @property
    def batch_size(self) -> int:
        return self._config['etl']['batch_size']
    
    @property
    def commit_interval(self) -> int:
        return self._config['etl']['commit_interval']
    
    @property
    def retry_attempts(self) -> int:
        return self._config['etl']['retry_attempts']
    
    @property
    def timeout_seconds(self) -> int:
        return self._config['etl']['timeout_seconds']
    
    # ID Prefixes
    @property
    def prefix_etl_run(self) -> str:
        return self._config['id_prefixes']['etl_run']
    
    @property
    def prefix_log_id(self) -> str:
        return self._config['id_prefixes']['log_id']
    
    @property
    def prefix_analytics_id(self) -> str:
        return self._config['id_prefixes']['analytics_id']
    
    # Data sources
    @property
    def raw_sales_path(self) -> str:
        return self._config['data_sources']['raw_sales_path']
    
    @property
    def analytics_output_path(self) -> str:
        return self._config['data_sources']['analytics_output_path']
    
    @property
    def log_output_path(self) -> str:
        return self._config['data_sources']['log_output_path']
    
    # Spark configuration
    @property
    def spark_app_name(self) -> str:
        return self._config['spark']['app_name']
    
    @property
    def spark_master(self) -> str:
        return self._config['spark']['master']
    
    @property
    def spark_log_level(self) -> str:
        return self._config['spark']['log_level']
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Path to config value (e.g., 'business_rules.tax_rate')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value