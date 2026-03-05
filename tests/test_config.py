"""
Unit tests for configuration management module.
"""

import pytest
from pathlib import Path
import yaml
from src.config import (
    ConfigurationManager,
    DatabaseConfig,
    SparkConfig,
    ETLConfig,
    BusinessRulesConfig,
    LoggingConfig,
    get_config_manager
)


@pytest.fixture
def sample_config_file(tmp_path):
    """Create sample configuration file."""
    config_data = {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'username': 'test_user',
            'password': 'test_pass',
            'driver': 'postgresql'
        },
        'spark': {
            'app_name': 'Test ETL',
            'master': 'local[2]'
        },
        'etl': {
            'batch_size': 500,
            'commit_interval': 250
        },
        'business_rules': {
            'discount_qty_tier1': 10,
            'tax_rate': 0.08
        },
        'logging': {
            'level': 'DEBUG',
            'log_dir': 'test_logs'
        }
    }
    
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_data, f)
    
    return config_file


@pytest.fixture
def config_manager(sample_config_file):
    """Create ConfigurationManager instance."""
    return ConfigurationManager(str(sample_config_file))


class TestConfigurationManager:
    """Test ConfigurationManager class."""

    def test_init_with_valid_file(self, sample_config_file):
        """Test initialization with valid config file."""
        manager = ConfigurationManager(str(sample_config_file))
        assert manager.config_path.exists()
        assert manager._config_data is not None

    def test_init_with_missing_file(self):
        """Test initialization with missing config file."""
        with pytest.raises(FileNotFoundError):
            ConfigurationManager("nonexistent.yaml")

    def test_get_database_config(self, config_manager):
        """Test getting database configuration."""
        db_config = config_manager.get_database_config()
        
        assert isinstance(db_config, DatabaseConfig)
        assert db_config.host == 'localhost'
        assert db_config.port == 5432
        assert db_config.database == 'test_db'
        assert db_config.username == 'test_user'
        assert db_config.password == 'test_pass'

    def test_get_spark_config(self, config_manager):
        """Test getting Spark configuration."""
        spark_config = config_manager.get_spark_config()
        
        assert isinstance(spark_config, SparkConfig)
        assert spark_config.app_name == 'Test ETL'
        assert spark_config.master == 'local[2]'

    def test_get_etl_config(self, config_manager):
        """Test getting ETL configuration."""
        etl_config = config_manager.get_etl_config()
        
        assert isinstance(etl_config, ETLConfig)
        assert etl_config.batch_size == 500
        assert etl_config.commit_interval == 250

    def test_get_business_rules_config(self, config_manager):
        """Test getting business rules configuration."""
        rules_config = config_manager.get_business_rules_config()
        
        assert isinstance(rules_config, BusinessRulesConfig)
        assert rules_config.discount_qty_tier1 == 10
        assert rules_config.tax_rate == 0.08

    def test_get_logging_config(self, config_manager):
        """Test getting logging configuration."""
        log_config = config_manager.get_logging_config()
        
        assert isinstance(log_config, LoggingConfig)
        assert log_config.level == 'DEBUG'
        assert log_config.log_dir == 'test_logs'

    def test_get_value_nested(self, config_manager):
        """Test getting nested configuration value."""
        value = config_manager.get_value('database.host')
        assert value == 'localhost'
        
        value = config_manager.get_value('etl.batch_size')
        assert value == 500

    def test_get_value_with_default(self, config_manager):
        """Test getting value with default."""
        value = config_manager.get_value('nonexistent.key', default='default_value')
        assert value == 'default_value'

    def test_get_raw_config(self, config_manager):
        """Test getting raw configuration."""
        raw_config = config_manager.get_raw_config()
        
        assert isinstance(raw_config, dict)
        assert 'database' in raw_config
        assert 'spark' in raw_config

    def test_validate_config_valid(self, config_manager):
        """Test configuration validation with valid config."""
        assert config_manager.validate_config() is True

    def test_validate_config_missing_section(self, tmp_path):
        """Test configuration validation with missing section."""
        incomplete_config = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'username': 'test_user',
                'password': 'test_pass'
            }
            # Missing other required sections
        }
        
        config_file = tmp_path / "incomplete_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(incomplete_config, f)
        
        manager = ConfigurationManager(str(config_file))
        with pytest.raises(ValueError, match="Missing required configuration section"):
            manager.validate_config()

    def test_validate_config_missing_db_field(self, tmp_path):
        """Test configuration validation with missing database field."""
        incomplete_config = {
            'database': {
                'host': 'localhost',
                'port': 5432
                # Missing required fields
            },
            'spark': {},
            'etl': {},
            'business_rules': {},
            'logging': {}
        }
        
        config_file = tmp_path / "incomplete_db_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(incomplete_config, f)
        
        manager = ConfigurationManager(str(config_file))
        with pytest.raises(ValueError, match="Missing required database field"):
            manager.validate_config()


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass."""

    def test_database_config_defaults(self):
        """Test DatabaseConfig with defaults."""
        config = DatabaseConfig(
            host='localhost',
            port=5432,
            database='test_db',
            username='user',
            password='pass'
        )
        
        assert config.driver == 'postgresql'
        assert config.pool_size == 10
        assert config.max_overflow == 20


class TestSparkConfig:
    """Test SparkConfig dataclass."""

    def test_spark_config_defaults(self):
        """Test SparkConfig with defaults."""
        config = SparkConfig()
        
        assert config.app_name == "Sales ETL System"
        assert config.master == "local[*]"
        assert config.executor_memory == "4g"


class TestETLConfig:
    """Test ETLConfig dataclass."""

    def test_etl_config_defaults(self):
        """Test ETLConfig with defaults."""
        config = ETLConfig()
        
        assert config.batch_size == 1000
        assert config.commit_interval == 500
        assert config.retry_attempts == 3


class TestBusinessRulesConfig:
    """Test BusinessRulesConfig dataclass."""

    def test_business_rules_config_defaults(self):
        """Test BusinessRulesConfig with defaults."""
        config = BusinessRulesConfig()
        
        assert config.discount_qty_tier1 == 10
        assert config.discount_rate_tier1 == 0.05
        assert config.tax_rate == 0.08


class TestLoggingConfig:
    """Test LoggingConfig dataclass."""

    def test_logging_config_defaults(self):
        """Test LoggingConfig with defaults."""
        config = LoggingConfig()
        
        assert config.level == "INFO"
        assert config.log_dir == "logs"
        assert config.console_output is True


def test_get_config_manager_singleton(sample_config_file):
    """Test singleton behavior of get_config_manager."""
    manager1 = get_config_manager(str(sample_config_file))
    manager2 = get_config_manager()
    
    assert manager1 is manager2