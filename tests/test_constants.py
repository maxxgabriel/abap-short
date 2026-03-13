"""
Unit Tests for Constants and Configuration

Tests enum values, configuration loading, and numeric precision.
"""

import pytest
from decimal import Decimal
from pathlib import Path
import tempfile
import yaml

from src.constants import (
    StatusCode, ProcessStep, SaleCategory, IDPrefix, MessageText, ETLDefaults
)
from src.config_loader import ConfigLoader, get_config


class TestEnums:
    """Test enum classes"""
    
    def test_status_code_values(self):
        """Test status code enum values match ABAP constants"""
        assert StatusCode.NEW == "N"
        assert StatusCode.PROCESSED == "P"
        assert StatusCode.ERROR == "E"
        assert StatusCode.WARNING == "W"
        assert StatusCode.SUCCESS == "S"
        assert StatusCode.INFO == "I"
    
    def test_process_step_values(self):
        """Test process step enum values match ABAP constants"""
        assert ProcessStep.INIT == "INIT"
        assert ProcessStep.EXTRACT == "EXTRACT"
        assert ProcessStep.TRANSFORM == "TRANSFORM"
        assert ProcessStep.LOAD == "LOAD"
        assert ProcessStep.VALIDATE == "VALIDATE"
        assert ProcessStep.COMPLETE == "COMPLETE"
        assert ProcessStep.ERROR == "ERROR"
    
    def test_sale_category_values(self):
        """Test sale category enum values match ABAP constants"""
        assert SaleCategory.HIGH == "HIGH"
        assert SaleCategory.MEDIUM == "MEDIUM"
        assert SaleCategory.LOW == "LOW"
    
    def test_id_prefix_values(self):
        """Test ID prefix enum values match ABAP constants"""
        assert IDPrefix.ETL_RUN == "ETL"
        assert IDPrefix.LOG == "LOG"
        assert IDPrefix.ANALYTICS == "ANL"


class TestMessageText:
    """Test message text constants"""
    
    def test_message_text_values(self):
        """Test message text constants match ABAP"""
        assert MessageText.INIT_SUCCESS == "ETL process initialized successfully"
        assert MessageText.EXTRACT_START == "Starting data extraction"
        assert MessageText.EXTRACT_COMPLETE == "Data extraction completed"
        assert MessageText.ETL_COMPLETE == "ETL process completed successfully"


class TestETLDefaults:
    """Test ETL default constants"""
    
    def test_default_values(self):
        """Test default values match ABAP constants"""
        assert ETLDefaults.BATCH_SIZE == 1000
        assert ETLDefaults.COMMIT_INTERVAL == 500
        assert ETLDefaults.RETRY_ATTEMPTS == 3
        assert ETLDefaults.TIMEOUT_SECONDS == 3600


class TestConfigLoader:
    """Test configuration loader"""
    
    @pytest.fixture
    def sample_config(self):
        """Create sample configuration"""
        return {
            'business_rules': {
                'discount': {
                    'quantity_tier1': 10,
                    'quantity_tier2': 15,
                    'rate_tier1': 0.05,
                    'rate_tier2': 0.10
                },
                'tax': {
                    'rate': 0.08
                },
                'cost': {
                    'ratio': 0.60
                },
                'category': {
                    'high_threshold': 2000.00,
                    'medium_threshold': 500.00
                }
            },
            'etl_configuration': {
                'batch_size': 1000,
                'retry_attempts': 3,
                'timeout_seconds': 3600
            }
        }
    
    @pytest.fixture
    def temp_config_file(self, sample_config):
        """Create temporary config file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        Path(temp_path).unlink()
    
    def test_load_config(self, temp_config_file):
        """Test configuration loading"""
        config = ConfigLoader(temp_config_file)
        
        assert config.get('business_rules.discount.quantity_tier1') == 10
        assert config.get('business_rules.tax.rate') == Decimal('0.08')
    
    def test_numeric_precision(self, temp_config_file):
        """Test numeric precision conversion to Decimal"""
        config = ConfigLoader(temp_config_file)
        
        # Verify Decimal type for precision
        assert isinstance(config.discount_rate_tier1, Decimal)
        assert isinstance(config.tax_rate, Decimal)
        assert isinstance(config.cost_ratio, Decimal)
        
        # Verify exact values
        assert config.discount_rate_tier1 == Decimal('0.05')
        assert config.discount_rate_tier2 == Decimal('0.10')
        assert config.tax_rate == Decimal('0.08')
        assert config.cost_ratio == Decimal('0.60')
    
    def test_property_accessors(self, temp_config_file):
        """Test property accessor methods"""
        config = ConfigLoader(temp_config_file)
        
        assert config.discount_qty_tier1 == 10
        assert config.discount_qty_tier2 == 15
        assert config.batch_size == 1000
        assert config.retry_attempts == 3
    
    def test_category_thresholds(self, temp_config_file):
        """Test category threshold values"""
        config = ConfigLoader(temp_config_file)
        
        assert config.category_high_threshold == Decimal('2000.00')
        assert config.category_medium_threshold == Decimal('500.00')
    
    def test_missing_config_file(self):
        """Test error handling for missing config file"""
        with pytest.raises(FileNotFoundError):
            ConfigLoader("nonexistent.yaml")
    
    def test_invalid_config_structure(self):
        """Test validation of config structure"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'invalid': 'structure'}, f)
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="Missing required configuration section"):
                ConfigLoader(temp_path)
        finally:
            Path(temp_path).unlink()
    
    def test_get_with_default(self, temp_config_file):
        """Test get method with default value"""
        config = ConfigLoader(temp_config_file)
        
        assert config.get('nonexistent.key', 'default') == 'default'
        assert config.get('business_rules.tax.rate') == Decimal('0.08')


class TestDecimalPrecision:
    """Test decimal precision handling"""
    
    def test_discount_calculation_precision(self):
        """Test discount calculation maintains precision"""
        from decimal import Decimal, getcontext
        
        # Set precision
        getcontext().prec = 28
        
        gross = Decimal('999.99')
        rate = Decimal('0.05')
        
        discount = gross * rate
        
        # Verify precision maintained
        assert discount == Decimal('49.9995')
        
        # Verify rounding
        discount_rounded = round(discount, 2)
        assert discount_rounded == Decimal('50.00')
    
    def test_tax_calculation_precision(self):
        """Test tax calculation maintains precision"""
        from decimal import Decimal
        
        amount = Decimal('950.00')
        tax_rate = Decimal('0.08')
        
        tax = amount * tax_rate
        
        assert tax == Decimal('76.00')
    
    def test_profit_margin_calculation(self):
        """Test profit margin calculation precision"""
        from decimal import Decimal
        
        net = Decimal('1000.00')
        cost = Decimal('600.00')
        
        profit_margin = ((net - cost) / net) * Decimal('100')
        
        assert profit_margin == Decimal('40.00')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])