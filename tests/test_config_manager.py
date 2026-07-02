"""
Unit tests for ConfigurationManager
"""

import pytest
import tempfile
import os
from pathlib import Path
from src.config_manager import ConfigurationManager, get_config


@pytest.fixture
def sample_config():
    """Create sample configuration content."""
    return """
app_name: "Test_ETL"
version: "1.0.0"

database:
  host: "localhost"
  port: 5432
  database: "test_db"
  user: "test_user"

etl:
  batch_size: 1000
  parallel_jobs: 4
  retry_attempts: 3

business_rules:
  discount:
    tier1:
      quantity_threshold: 10
      rate: 0.05
  tax:
    rate: 0.08

logging:
  level: "INFO"
  log_dir: "logs"
"""


@pytest.fixture
def config_file(sample_config):
    """Create temporary config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(sample_config)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


class TestConfigurationManager:
    """Test cases for ConfigurationManager."""
    
    def test_load_config_success(self, config_file):
        """Test successful configuration loading."""
        config_mgr = ConfigurationManager(config_file)
        
        assert config_mgr.get("app_name") == "Test_ETL"
        assert config_mgr.get("version") == "1.0.0"
    
    def test_load_config_file_not_found(self):
        """Test error when config file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            ConfigurationManager("nonexistent.yaml")
    
    def test_get_nested_value_dot_notation(self, config_file):
        """Test getting nested values with dot notation."""
        config_mgr = ConfigurationManager(config_file)
        
        assert config_mgr.get("database.host") == "localhost"
        assert config_mgr.get("database.port") == 5432
        assert config_mgr.get("etl.batch_size") == 1000
    
    def test_get_with_default(self, config_file):
        """Test getting value with default."""
        config_mgr = ConfigurationManager(config_file)
        
        result = config_mgr.get("nonexistent.key", "default_value")
        assert result == "default_value"
    
    def test_get_database_config(self, config_file):
        """Test getting database configuration."""
        config_mgr = ConfigurationManager(config_file)
        db_config = config_mgr.get_database_config()
        
        assert db_config["host"] == "localhost"
        assert db_config["port"] == 5432
        assert db_config["database"] == "test_db"
    
    def test_get_etl_config(self, config_file):
        """Test getting ETL configuration."""
        config_mgr = ConfigurationManager(config_file)
        etl_config = config_mgr.get_etl_config()
        
        assert etl_config["batch_size"] == 1000
        assert etl_config["parallel_jobs"] == 4
        assert etl_config["retry_attempts"] == 3
    
    def test_get_business_rules(self, config_file):
        """Test getting business rules configuration."""
        config_mgr = ConfigurationManager(config_file)
        rules = config_mgr.get_business_rules()
        
        assert rules["discount"]["tier1"]["quantity_threshold"] == 10
        assert rules["discount"]["tier1"]["rate"] == 0.05
        assert rules["tax"]["rate"] == 0.08
    
    def test_get_logging_config(self, config_file):
        """Test getting logging configuration."""
        config_mgr = ConfigurationManager(config_file)
        log_config = config_mgr.get_logging_config()
        
        assert log_config["level"] == "INFO"
        assert log_config["log_dir"] == "logs"
    
    def test_get_all_config(self, config_file):
        """Test getting entire configuration."""
        config_mgr = ConfigurationManager(config_file)
        all_config = config_mgr.get_all()
        
        assert "app_name" in all_config
        assert "database" in all_config
        assert "etl" in all_config
    
    def test_env_override_database(self, config_file, monkeypatch):
        """Test environment variable override for database config."""
        monkeypatch.setenv("DB_HOST", "prod-server")
        monkeypatch.setenv("DB_PORT", "5433")
        
        config_mgr = ConfigurationManager(config_file)
        
        assert config_mgr.get("database.host") == "prod-server"
        assert config_mgr.get("database.port") == 5433
    
    def test_env_override_etl(self, config_file, monkeypatch):
        """Test environment variable override for ETL config."""
        monkeypatch.setenv("BATCH_SIZE", "2000")
        monkeypatch.setenv("PARALLEL_JOBS", "8")
        
        config_mgr = ConfigurationManager(config_file)
        
        assert config_mgr.get("etl.batch_size") == 2000
        assert config_mgr.get("etl.parallel_jobs") == 8
    
    def test_reload_config(self, config_file):
        """Test reloading configuration."""
        config_mgr = ConfigurationManager(config_file)
        
        original_value = config_mgr.get("app_name")
        
        # Modify the config file
        with open(config_file, 'a') as f:
            f.write("\nnew_key: 'new_value'\n")
        
        config_mgr.reload()
        
        assert config_mgr.get("app_name") == original_value
        assert config_mgr.get("new_key") == "new_value"
    
    def test_get_config_singleton(self, config_file):
        """Test global config instance."""
        # Reset global instance
        import src.config_manager as cm
        cm._config_instance = None
        
        config1 = get_config(config_file)
        config2 = get_config()
        
        assert config1 is config2