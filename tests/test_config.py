"""
Unit tests for configuration loading and validation
"""
import pytest
import yaml
from pathlib import Path


class TestConfiguration:
    """Test configuration file structure and values"""
    
    @pytest.fixture
    def config(self):
        """Load configuration file"""
        config_path = Path(__file__).parent.parent / 'config.yaml'
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def test_config_loads(self, config):
        """Test that config file loads successfully"""
        assert config is not None
        assert isinstance(config, dict)
    
    def test_status_codes(self, config):
        """Test status code configuration"""
        status_codes = config.get('status_codes', {})
        
        assert status_codes['new'] == 'N'
        assert status_codes['processed'] == 'P'
        assert status_codes['error'] == 'E'
        assert status_codes['warning'] == 'W'
        assert status_codes['success'] == 'S'
        assert status_codes['info'] == 'I'
    
    def test_process_steps(self, config):
        """Test process step configuration"""
        steps = config.get('process_steps', {})
        
        assert steps['init'] == 'INIT'
        assert steps['extract'] == 'EXTRACT'
        assert steps['transform'] == 'TRANSFORM'
        assert steps['load'] == 'LOAD'
        assert steps['validate'] == 'VALIDATE'
        assert steps['complete'] == 'COMPLETE'
        assert steps['error'] == 'ERROR'
    
    def test_business_rules(self, config):
        """Test business rules configuration"""
        discount = config.get('discount_rules', {})
        tax = config.get('tax_rules', {})
        cost = config.get('cost_rules', {})
        categories = config.get('category_thresholds', {})
        
        # Discount rules
        assert discount['quantity_tier1'] == 10
        assert discount['quantity_tier2'] == 15
        assert discount['rate_tier1'] == 0.05
        assert discount['rate_tier2'] == 0.10
        
        # Tax rules
        assert tax['tax_rate'] == 0.08
        
        # Cost rules
        assert cost['cost_ratio'] == 0.60
        
        # Category thresholds
        assert categories['high'] == 2000.00
        assert categories['medium'] == 500.00
    
    def test_etl_config(self, config):
        """Test ETL configuration parameters"""
        etl_config = config.get('etl_config', {})
        
        assert etl_config['default_batch_size'] == 1000
        assert etl_config['default_commit_interval'] == 500
        assert etl_config['default_retry_attempts'] == 3
        assert etl_config['default_timeout_seconds'] == 3600
        assert etl_config['parallel_jobs'] == 4
    
    def test_id_prefixes(self, config):
        """Test ID prefix configuration"""
        prefixes = config.get('id_prefixes', {})
        
        assert prefixes['etl_run'] == 'ETL'
        assert prefixes['log_id'] == 'LOG'
        assert prefixes['analytics_id'] == 'ANL'
    
    def test_spark_config(self, config):
        """Test Spark configuration"""
        spark_config = config.get('spark', {})
        
        assert spark_config['app_name'] == 'Sales ETL System'
        assert 'master' in spark_config
        assert 'config' in spark_config
        
        spark_settings = spark_config['config']
        assert 'spark.sql.shuffle.partitions' in spark_settings
        assert 'spark.executor.memory' in spark_settings


if __name__ == '__main__':
    pytest.main([__file__, '-v'])