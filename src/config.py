"""
ETL Configuration Management
Replaces ABAP constants and configuration
"""
from dataclasses import dataclass, field
from typing import Dict, Any
import yaml


@dataclass
class StatusCodes:
    """Status code constants - replaces gc_status from ZCL_ETL_CONSTANTS"""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass
class ProcessSteps:
    """Process step constants - replaces gc_step from ZCL_ETL_CONSTANTS"""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass
class SaleCategories:
    """Sale category constants - replaces gc_category from ZCL_ETL_CONSTANTS"""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'


@dataclass
class BusinessRules:
    """Business rule configuration - replaces business rule constants"""
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: float = 0.05
    discount_rate_tier2: float = 0.10
    tax_rate: float = 0.08
    cost_ratio: float = 0.60
    category_high_threshold: float = 2000.00
    category_medium_threshold: float = 500.00


@dataclass
class ETLDefaults:
    """ETL configuration defaults"""
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600


@dataclass
class IDPrefixes:
    """ID prefix constants"""
    ETL_RUN: str = 'ETL'
    LOG_ID: str = 'LOG'
    ANALYTICS_ID: str = 'ANL'


@dataclass
class Messages:
    """Standard message texts"""
    INIT_SUCCESS: str = 'ETL process initialized successfully'
    EXTRACT_START: str = 'Starting data extraction'
    EXTRACT_COMPLETE: str = 'Data extraction completed'
    TRANSFORM_START: str = 'Starting data transformation'
    TRANSFORM_COMPLETE: str = 'Data transformation completed'
    LOAD_START: str = 'Starting data load'
    LOAD_COMPLETE: str = 'Data load completed'
    ETL_COMPLETE: str = 'ETL process completed successfully'
    ETL_ERROR: str = 'ETL process failed'


@dataclass
class ETLConfig:
    """Main ETL configuration class"""
    status: StatusCodes = field(default_factory=StatusCodes)
    steps: ProcessSteps = field(default_factory=ProcessSteps)
    categories: SaleCategories = field(default_factory=SaleCategories)
    business_rules: BusinessRules = field(default_factory=BusinessRules)
    defaults: ETLDefaults = field(default_factory=ETLDefaults)
    prefixes: IDPrefixes = field(default_factory=IDPrefixes)
    messages: Messages = field(default_factory=Messages)
    
    # Database/storage paths
    source_path: str = "data/raw/sales"
    target_path: str = "data/analytics/sales"
    log_path: str = "data/logs/etl"
    
    @classmethod
    def from_yaml(cls, config_path: str) -> 'ETLConfig':
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            ETLConfig instance
        """
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        config = cls()
        
        # Update business rules if provided
        if 'business_rules' in config_dict:
            for key, value in config_dict['business_rules'].items():
                if hasattr(config.business_rules, key):
                    setattr(config.business_rules, key, value)
        
        # Update defaults if provided
        if 'defaults' in config_dict:
            for key, value in config_dict['defaults'].items():
                if hasattr(config.defaults, key):
                    setattr(config.defaults, key, value)
        
        # Update paths if provided
        if 'paths' in config_dict:
            for key, value in config_dict['paths'].items():
                if hasattr(config, f"{key}_path"):
                    setattr(config, f"{key}_path", value)
        
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'business_rules': {
                'discount_qty_tier1': self.business_rules.discount_qty_tier1,
                'discount_qty_tier2': self.business_rules.discount_qty_tier2,
                'discount_rate_tier1': self.business_rules.discount_rate_tier1,
                'discount_rate_tier2': self.business_rules.discount_rate_tier2,
                'tax_rate': self.business_rules.tax_rate,
                'cost_ratio': self.business_rules.cost_ratio,
                'category_high_threshold': self.business_rules.category_high_threshold,
                'category_medium_threshold': self.business_rules.category_medium_threshold,
            },
            'defaults': {
                'batch_size': self.defaults.batch_size,
                'commit_interval': self.defaults.commit_interval,
                'retry_attempts': self.defaults.retry_attempts,
                'timeout_seconds': self.defaults.timeout_seconds,
            },
            'paths': {
                'source': self.source_path,
                'target': self.target_path,
                'log': self.log_path,
            }
        }