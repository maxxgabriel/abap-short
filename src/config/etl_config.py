```python
import os
import yaml
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    jdbc_url: str
    driver: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls):
        return cls(
            jdbc_url=os.getenv('DB_JDBC_URL', 'jdbc:postgresql://localhost:5432/etl_db'),
            driver=os.getenv('DB_DRIVER', 'org.postgresql.Driver'),
            user=os.getenv('DB_USER', 'etl_user'),
            password=os.getenv('DB_PASSWORD', 'etl_password')
        )

@dataclass
class BusinessRules:
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
    database: DatabaseConfig
    business_rules: BusinessRules
    batch_size: int = 1000
    commit_interval: int = 500
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    test_mode: bool = False
    
    @classmethod
    def load_from_yaml(cls, config_path: str):
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        db_config = DatabaseConfig(
            jdbc_url=config_data.get('database', {}).get('jdbc_url', ''),
            driver=config_data.get('database', {}).get('driver', ''),
            user=config_data.get('database', {}).get('user', ''),
            password=config_data.get('database', {}).get('password', '')
        )
        
        rules_data = config_data.get('business_rules', {})
        business_rules = BusinessRules(
            discount_qty_tier1=rules_data.get('discount_qty_tier1', 10),
            discount_qty_tier2=rules_data.get('discount_qty_tier2', 15),
            discount_rate_tier1=rules_data.get('discount_rate_tier1', 0.05),
            discount_rate_tier2=rules_data.get('discount_rate_tier2', 0.10),
            tax_rate=rules_data.get('tax_rate', 0.08),
            cost_ratio=rules_data.get('cost_ratio', 0.60),
            category_high_threshold=rules_data.get('category_high_threshold', 2000.00),
            category_medium_threshold=rules_data.get('category_medium_threshold', 500.00)
        )
        
        return cls(
            database=db_config,
            business_rules=business_rules,
            batch_size=config_data.get('batch_size', 1000),
            commit_interval=config_data.get('commit_interval', 500),
            retry_attempts=config_data.get('retry_attempts', 3),
            timeout_seconds=config_data.get('timeout_seconds', 3600),
            test_mode=config_data.get('test_mode', False)
        )
    
    @classmethod
    def load_default(cls):
        return cls(
            database=DatabaseConfig.from_env(),
            business_rules=BusinessRules()
        )
```