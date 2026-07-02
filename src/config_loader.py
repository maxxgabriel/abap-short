"""
Configuration Loader

Loads and validates YAML configuration with proper numeric precision handling.
"""

from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict
import yaml


# Set decimal precision globally for financial calculations
getcontext().prec = 28


class ConfigLoader:
    """Loads and validates ETL configuration from YAML"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration loader
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self._config = yaml.safe_load(f)
        
        self._validate_config()
        self._convert_numeric_precision()
    
    def _validate_config(self) -> None:
        """Validate required configuration sections"""
        required_sections = ['business_rules', 'etl_configuration']
        
        for section in required_sections:
            if section not in self._config:
                raise ValueError(f"Missing required configuration section: {section}")
    
    def _convert_numeric_precision(self) -> None:
        """Convert numeric values to Decimal for precision"""
        # Convert business rule values to Decimal
        rules = self._config.get('business_rules', {})
        
        if 'discount' in rules:
            rules['discount']['rate_tier1'] = Decimal(str(rules['discount']['rate_tier1']))
            rules['discount']['rate_tier2'] = Decimal(str(rules['discount']['rate_tier2']))
        
        if 'tax' in rules:
            rules['tax']['rate'] = Decimal(str(rules['tax']['rate']))
        
        if 'cost' in rules:
            rules['cost']['ratio'] = Decimal(str(rules['cost']['ratio']))
        
        if 'category' in rules:
            rules['category']['high_threshold'] = Decimal(str(rules['category']['high_threshold']))
            rules['category']['medium_threshold'] = Decimal(str(rules['category']['medium_threshold']))
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        
        Args:
            key: Dot-notation key (e.g., 'business_rules.tax.rate')
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
    
    def get_business_rule(self, rule_path: str) -> Any:
        """
        Get business rule value
        
        Args:
            rule_path: Path within business_rules (e.g., 'tax.rate')
            
        Returns:
            Business rule value
        """
        return self.get(f'business_rules.{rule_path}')
    
    def get_etl_config(self, config_key: str) -> Any:
        """
        Get ETL configuration value
        
        Args:
            config_key: Configuration key
            
        Returns:
            Configuration value
        """
        return self.get(f'etl_configuration.{config_key}')
    
    @property
    def discount_qty_tier1(self) -> int:
        """Discount quantity threshold tier 1"""
        return self.get_business_rule('discount.quantity_tier1')
    
    @property
    def discount_qty_tier2(self) -> int:
        """Discount quantity threshold tier 2"""
        return self.get_business_rule('discount.quantity_tier2')
    
    @property
    def discount_rate_tier1(self) -> Decimal:
        """Discount rate tier 1"""
        return self.get_business_rule('discount.rate_tier1')
    
    @property
    def discount_rate_tier2(self) -> Decimal:
        """Discount rate tier 2"""
        return self.get_business_rule('discount.rate_tier2')
    
    @property
    def tax_rate(self) -> Decimal:
        """Tax rate"""
        return self.get_business_rule('tax.rate')
    
    @property
    def cost_ratio(self) -> Decimal:
        """Cost ratio"""
        return self.get_business_rule('cost.ratio')
    
    @property
    def category_high_threshold(self) -> Decimal:
        """High category threshold"""
        return self.get_business_rule('category.high_threshold')
    
    @property
    def category_medium_threshold(self) -> Decimal:
        """Medium category threshold"""
        return self.get_business_rule('category.medium_threshold')
    
    @property
    def batch_size(self) -> int:
        """Batch size for processing"""
        return self.get_etl_config('batch_size')
    
    @property
    def retry_attempts(self) -> int:
        """Number of retry attempts"""
        return self.get_etl_config('retry_attempts')
    
    @property
    def timeout_seconds(self) -> int:
        """Timeout in seconds"""
        return self.get_etl_config('timeout_seconds')


# Singleton instance
_config_instance: ConfigLoader = None


def get_config(config_path: str = "config.yaml") -> ConfigLoader:
    """
    Get singleton configuration instance
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        ConfigLoader instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigLoader(config_path)
    
    return _config_instance