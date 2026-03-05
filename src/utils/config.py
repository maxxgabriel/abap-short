"""
Configuration Management
Handles loading and accessing configuration from YAML files
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class Config:
    """Configuration manager for ETL system"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration
        
        Args:
            config_path: Path to config file (default: config.yaml)
        """
        if config_path is None:
            # Look for config.yaml in project root
            config_path = Path(__file__).parent.parent.parent / "config.yaml"
        
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
        
        # Replace environment variables
        self._replace_env_vars(self._config)
    
    def _replace_env_vars(self, obj: Any) -> Any:
        """Recursively replace ${VAR} with environment variables"""
        if isinstance(obj, dict):
            return {k: self._replace_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._replace_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        return obj
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation key
        
        Args:
            key: Configuration key (e.g., 'etl.batch_size')
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration as dictionary"""
        spark_conf = self.get('spark.config', {})
        return {k: str(v) for k, v in spark_conf.items()}
    
    def get_business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration"""
        return self.get('business_rules', {})
    
    def get_data_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get data source configuration"""
        return self.get(f'data_sources.{source_name}', {})
    
    def get_data_target_config(self, target_name: str) -> Dict[str, Any]:
        """Get data target configuration"""
        return self.get(f'data_targets.{target_name}', {})
    
    @property
    def etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration"""
        return self.get('etl', {})
    
    @property
    def spark_master(self) -> str:
        """Get Spark master URL"""
        return self.get('spark.master', 'local[*]')
    
    @property
    def app_name(self) -> str:
        """Get application name"""
        return self.get('spark.app_name', 'Sales ETL System')