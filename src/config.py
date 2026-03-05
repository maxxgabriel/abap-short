"""
ETL Configuration Management
Loads and manages configuration from YAML files
"""
import yaml
import os
from typing import Any, Dict


class ETLConfig:
    """Configuration manager for ETL process"""
    
    def __init__(self, config_path: str = "config.yaml", env: str = None):
        self.config_path = config_path
        self.env = env or os.getenv("ETL_ENV", "development")
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            all_config = yaml.safe_load(f)
        
        # Get environment-specific config
        env_config = all_config.get(self.env, {})
        common_config = all_config.get("common", {})
        
        # Merge configs (env-specific overrides common)
        merged_config = {**common_config, **env_config}
        
        return merged_config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Args:
            key: Configuration key (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark-specific configuration"""
        return self.get("spark", {})
    
    def get_source_config(self) -> Dict[str, Any]:
        """Get source configuration"""
        return self.get("source", {})
    
    def get_target_config(self) -> Dict[str, Any]:
        """Get target configuration"""
        return self.get("target", {})