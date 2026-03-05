"""
Unit tests for ETL configuration module.
Tests constant mappings, type conversions, and configuration loading.
"""

import pytest
from decimal import Decimal
from pathlib import Path
import tempfile
import yaml

from src.config import (
    ETLConstants,
    ETLConfig,
    StatusCode,
    ProcessStep,
    SaleCategory,
    BusinessRules,
    ETLDefaults,
    IDPrefixes,
    Messages,
    get_config,
    reset_config
)


class TestEnumerations:
    """Test enum mappings from ABAP structures"""
    
    def test_status_code_values(self):
        """Test status code enum matches ABAP gc_status"""
        assert StatusCode.NEW.value == 'N'
        assert StatusCode.PROCESSED.value == 'P'
        assert StatusCode.ERROR.value == 'E'
        assert StatusCode.WARNING.value == 'W'
        assert StatusCode.SUCCESS.value == 'S'
        assert StatusCode.INFO.value == 'I'
    
    def test_process_step_values(self):
        """Test process step enum matches ABAP gc_step"""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'
    
    def test_sale_category_values(self):
        """Test sale category enum matches ABAP gc_category"""
        assert SaleCategory.HIGH.value == 'HIGH'
        assert SaleCategory.MEDIUM.value == 'MEDIUM'
        assert SaleCategory.LOW.value == 'LOW'


class TestBusinessRules:
    """Test business rules configuration class"""
    
    def test_default_values(self):
        """Test business rules have correct default values"""
        rules = BusinessRules()
        
        # Integer thresholds (ABAP TYPE i)
        assert rules.discount_qty_tier1 == 10
        assert rules.discount_qty_tier2 == 15
        
        # Decimal rates (ABAP TYPE p LENGTH 3 DECIMALS 2)
        assert rules.discount_rate_tier1 == Decimal('0.05')
        assert rules.discount_rate_tier2 == Decimal('0.10')
        assert rules.tax_rate == Decimal('0.08')
        assert rules.cost_ratio == Decimal('0.60')
        
        # Decimal thresholds (ABAP TYPE p LENGTH 16 DECIMALS 2)
        assert rules.category_high_threshold == Decimal('2000.00')
        assert rules.category_medium_threshold == Decimal('500.00')
    
    def test_decimal_precision(self):
        """Test Decimal types maintain precision"""
        rules = BusinessRules()
        
        # Verify Decimal type
        assert isinstance(rules.discount_rate_tier1, Decimal)
        assert isinstance(rules.tax_rate, Decimal)
        
        # Verify precision preservation
        rate = rules.discount_rate_tier1
        assert str(rate) == '0.05'
        assert rate.as_tuple().exponent == -2
    
    def test_immutability(self):
        """Test BusinessRules is immutable (frozen dataclass)"""
        rules = BusinessRules()
        
        with pytest.raises(Exception):  # FrozenInstanceError
            rules.tax_rate = Decimal('0.10')


class TestETLDefaults:
    """Test ETL defaults configuration"""
    
    def test_default_values(self):
        """Test ETL defaults match ABAP constants"""
        defaults = ETLDefaults()
        
        assert defaults.batch_size == 1000
        assert defaults.commit_interval == 500
        assert defaults.retry_attempts == 3
        assert defaults.timeout_seconds == 3600
        assert defaults.parallel_jobs == 4
    
    def test_all_integers(self):
        """Test all default values are integers (ABAP TYPE i)"""
        defaults = ETLDefaults()
        
        assert isinstance(defaults.batch_size, int)
        assert isinstance(defaults.commit_interval, int)
        assert isinstance(defaults.retry_attempts, int)
        assert isinstance(defaults.timeout_seconds, int)


class TestIDPrefixes:
    """Test ID prefix constants"""
    
    def test_prefix_values(self):
        """Test ID prefixes match ABAP constants"""
        prefixes = IDPrefixes()
        
        assert prefixes.etl_run == 'ETL'
        assert prefixes.log_id == 'LOG'
        assert prefixes.analytics_id == 'ANL'


class TestMessages:
    """Test message text constants"""
    
    def test_message_texts(self):
        """Test message texts match ABAP constants"""
        messages = Messages()
        
        assert messages.init_success == 'ETL process initialized successfully'
        assert messages.extract_start == 'Starting data extraction'
        assert messages.extract_complete == 'Data extraction completed'
        assert messages.transform_start == 'Starting data transformation'
        assert messages.transform_complete == 'Data transformation completed'
        assert messages.load_start == 'Starting data load'
        assert messages.load_complete == 'Data load completed'
        assert messages.etl_complete == 'ETL process completed successfully'
        assert messages.etl_error == 'ETL process failed'


class TestETLConstants:
    """Test main ETL constants class"""
    
    def test_static_attributes(self):
        """Test static attributes are accessible"""
        assert ETLConstants.STATUS == StatusCode
        assert ETLConstants.STEP == ProcessStep
        assert ETLConstants.CATEGORY == SaleCategory
        assert isinstance(ETLConstants.BUSINESS_RULES, BusinessRules)
        assert isinstance(ETLConstants.DEFAULTS, ETLDefaults)
        assert isinstance(ETLConstants.PREFIXES, IDPrefixes)
        assert isinstance(ETLConstants.MESSAGES, Messages)
    
    def test_get_status_dict(self):
        """Test status dictionary conversion"""
        status_dict = ETLConstants.get_status_dict()
        
        assert status_dict['NEW'] == 'N'
        assert status_dict['PROCESSED'] == 'P'
        assert status_dict['ERROR'] == 'E'
        assert len(status_dict) == 6
    
    def test_get_step_dict(self):
        """Test step dictionary conversion"""
        step_dict = ETLConstants.get_step_dict()
        
        assert step_dict['INIT'] == 'INIT'
        assert step_dict['EXTRACT'] == 'EXTRACT'
        assert step_dict['TRANSFORM'] == 'TRANSFORM'
        assert len(step_dict) == 7
    
    def test_get_category_dict(self):
        """Test category dictionary conversion"""
        category_dict = ETLConstants.get_category_dict()
        
        assert category_dict['HIGH'] == 'HIGH'
        assert category_dict['MEDIUM'] == 'MEDIUM'
        assert category_dict['LOW'] == 'LOW'
        assert len(category_dict) == 3
    
    def test_to_dict(self):
        """Test full dictionary export"""
        config_dict = ETLConstants.to_dict()
        
        assert 'status' in config_dict
        assert 'step' in config_dict
        assert 'category' in config_dict
        assert 'business_rules' in config_dict
        assert 'defaults' in config_dict
        assert 'prefixes' in config_dict
        assert 'messages' in config_dict
        
        # Verify nested structure
        assert config_dict['business_rules']['tax_rate'] == '0.08'
        assert config_dict['defaults']['batch_size'] == 1000
        assert config_dict['prefixes']['etl_run'] == 'ETL'


class TestETLConfig:
    """Test runtime configuration class"""
    
    def test_default_initialization(self):
        """Test ETLConfig initializes with defaults from ETLConstants"""
        config = ETLConfig()
        
        assert config.batch_size == ETLConstants.DEFAULTS.batch_size
        assert config.tax_rate == ETLConstants.BUSINESS_RULES.tax_rate
        assert config.spark_app_name == "SalesETL"
    
    def test_from_yaml(self):
        """Test loading configuration from YAML file"""
        # Create temporary YAML config
        config_data = {
            'batch_size': 2000,
            'tax_rate': '0.10',
            'spark_app_name': 'TestETL',
            'raw_data_path': '/test/path',
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            config = ETLConfig.from_yaml(temp_path)
            
            assert config.batch_size == 2000
            assert config.tax_rate == Decimal('0.10')
            assert config.spark_app_name == 'TestETL'
            assert config.raw_data_path == '/test/path'
            
            # Verify non-overridden values use defaults
            assert config.commit_interval == ETLConstants.DEFAULTS.commit_interval
        finally:
            Path(temp_path).unlink()
    
    def test_to_yaml(self):
        """Test exporting configuration to YAML file"""
        config = ETLConfig(
            batch_size=1500,
            tax_rate=Decimal('0.09')
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = f.name
        
        try:
            config.to_yaml(temp_path)
            
            # Reload and verify
            with open(temp_path, 'r') as f:
                loaded_data = yaml.safe_load(f)
            
            assert loaded_data['batch_size'] == 1500
            assert loaded_data['tax_rate'] == '0.09'
        finally:
            Path(temp_path).unlink()
    
    def test_decimal_conversion(self):
        """Test proper Decimal conversion from YAML"""
        config_data = {
            'tax_rate': '0.08',
            'cost_ratio': '0.60',
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            config = ETLConfig.from_yaml(temp_path)
            
            assert isinstance(config.tax_rate, Decimal)
            assert isinstance(config.cost_ratio, Decimal)
            assert config.tax_rate == Decimal('0.08')
        finally:
            Path(temp_path).unlink()


class TestSingletonConfig:
    """Test singleton configuration management"""
    
    def setup_method(self):
        """Reset config before each test"""
        reset_config()
    
    def test_get_config_default(self):
        """Test getting default config without YAML"""
        config = get_config()
        
        assert isinstance(config, ETLConfig)
        assert config.batch_size == ETLConstants.DEFAULTS.batch_size
    
    def test_get_config_from_yaml(self):
        """Test getting config from YAML file"""
        config_data = {'batch_size': 3000}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            config = get_config(temp_path)
            assert config.batch_size == 3000
        finally:
            Path(temp_path).unlink()
    
    def test_singleton_behavior(self):
        """Test singleton returns same instance"""
        config1 = get_config()
        config2 = get_config()
        
        assert config1 is config2
    
    def test_reset_config(self):
        """Test resetting singleton"""
        config1 = get_config()
        reset_config()
        config2 = get_config()
        
        assert config1 is not config2


class TestTypeMapping:
    """Test ABAP to Python type mappings"""
    
    def test_integer_mapping(self):
        """Test ABAP TYPE i maps to Python int"""
        assert isinstance(ETLConstants.BUSINESS_RULES.discount_qty_tier1, int)
        assert isinstance(ETLConstants.DEFAULTS.batch_size, int)
    
    def test_decimal_mapping(self):
        """Test ABAP TYPE p maps to Python Decimal"""
        assert isinstance(ETLConstants.BUSINESS_RULES.tax_rate, Decimal)
        assert isinstance(ETLConstants.BUSINESS_RULES.category_high_threshold, Decimal)
    
    def test_string_mapping(self):
        """Test ABAP TYPE char/string maps to Python str"""
        assert isinstance(ETLConstants.PREFIXES.etl_run, str)
        assert isinstance(ETLConstants.MESSAGES.init_success, str)
    
    def test_structure_mapping(self):
        """Test ABAP structures map to Python Enums/dataclasses"""
        # ABAP structure -> Python Enum
        assert isinstance(StatusCode.NEW, StatusCode)
        
        # ABAP structure -> Python dataclass
        assert isinstance(ETLConstants.BUSINESS_RULES, BusinessRules)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])