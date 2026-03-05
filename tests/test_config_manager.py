"""
Unit tests for Configuration Manager
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from src.config_manager import ConfigManager


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'spark': {
            'app_name': 'Test_ETL',
            'master': 'local[*]'
        },
        'database': {
            'jdbc': {
                'host': 'localhost',
                'port': 5432
            }
        },
        'etl': {
            'batch_size': 500,
            'retry_attempts': 2
        },
        'logging': {
            'level': 'DEBUG',
            'log_dir': 'test_logs'
        },
        'business_rules': {
            'tax_rate': 0.10,
            'category': {
                'high_threshold': 1000.00
            }
        }
    }


@pytest.fixture
def config_file(sample_config):
    """Create temporary config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_config, f)
        config_path = f.name
    
    yield config_path
    
    # Cleanup
    Path(config_path).unlink()


class TestConfigManager:
    """Test cases for ConfigManager class."""
    
    def test_singleton_pattern(self):
        """Test that ConfigManager implements singleton pattern."""
        config1 = ConfigManager()
        config2 = ConfigManager()
        assert config1 is config2
    
    def test_load_config_success(self, config_file):
        """Test successful configuration loading."""
        config = ConfigManager()
        config._load_config(config_file)
        
        assert config.get('spark.app_name') == 'Test_ETL'
        assert config.get('database.jdbc.host') == 'localhost'
    
    def test_load_config_file_not_found(self):
        """Test configuration loading with non-existent file."""
        config = ConfigManager()
        
        with pytest.raises(FileNotFoundError):
            config._load_config('nonexistent.yaml')
    
    def test_get_with_dot_notation(self, config_file):
        """Test getting config values with dot notation."""
        config = ConfigManager()
        config._load_config(config_file)
        
        assert config.get('spark.app_name') == 'Test_ETL'
        assert config.get('database.jdbc.port') == 5432
        assert config.get('etl.batch_size') == 500
    
    def test_get_with_default(self, config_file):
        """Test getting config with default value."""
        config = ConfigManager()
        config._load_config(config_file)
        
        assert config.get('nonexistent.key', 'default_value') == 'default_value'
        assert config.get('spark.nonexistent', 100) == 100
    
    def test_get_spark_config(self, config_file):
        """Test getting Spark configuration section."""
        config = ConfigManager()
        config._load_config(config_file)
        
        spark_config = config.get_spark_config()
        assert spark_config['app_name'] == 'Test_ETL'
        assert spark_config['master'] == 'local[*]'
    
    def test_get_database_config(self, config_file):
        """Test getting database configuration section."""
        config = ConfigManager()
        config._load_config(config_file)
        
        db_config = config.get_database_config()
        assert db_config['jdbc']['host'] == 'localhost'
        assert db_config['jdbc']['port'] == 5432
    
    def test_get_etl_config(self, config_file):
        """Test getting ETL configuration section."""
        config = ConfigManager()
        config._load_config(config_file)
        
        etl_config = config.get_etl_config()
        assert etl_config['batch_size'] == 500
        assert etl_config['retry_attempts'] == 2
    
    def test_get_batch_size(self, config_file):
        """Test getting batch size configuration."""
        config = ConfigManager()
        config._load_config(config_file)
        
        assert config.get_batch_size() == 500
    
    def test_get_batch_size_default(self):
        """Test batch size with default value."""
        config = ConfigManager()
        config._config = {'etl': {}}  # No batch_size configured
        
        assert config.get_batch_size() == 1000  # Default value
    
    def test_get_retry_attempts(self, config_file):
        """Test getting retry attempts configuration."""
        config = ConfigManager()
        config._load_config(config_file)
        
        assert config.get_retry_attempts() == 2
    
    def test_get_category_thresholds(self, config_file):
        """Test getting category thresholds."""
        config = ConfigManager()
        config._load_config(config_file)
        
        thresholds = config.get_category_thresholds()
        assert thresholds['high'] == 1000.00
        assert 'medium' in thresholds
    
    def test_get_tax_rate(self, config_file):
        """Test getting tax rate."""
        config = ConfigManager()
        config._load_config(config_file)
        
        assert config.get_tax_rate() == 0.10
    
    def test_validate_config_missing_sections(self):
        """Test configuration validation with missing sections."""
        config = ConfigManager()
        config._config = {
            'spark': {},
            'database': {}
            # Missing 'etl' and 'logging' sections
        }
        
        with pytest.raises(ValueError, match="Missing required configuration sections"):
            config._validate_config()
    
    def test_reload_config(self, config_file):
        """Test reloading configuration."""
        config = ConfigManager()
        config._load_config(config_file)
        
        original_batch_size = config.get_batch_size()
        
        # Reload with same file
        config.reload_config(config_file)
        
        assert config.get_batch_size() == original_batch_size


if __name__ == '__main__':
    pytest.main([__file__, '-v'])