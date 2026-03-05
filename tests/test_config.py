"""
Unit tests for ETL configuration module.
Tests constant definitions, business rules, and configuration access.
"""

import pytest
from decimal import Decimal
from src.config import (
    ETLConstants, StatusCode, ProcessStep, SaleCategory,
    BusinessRules, ETLConfig, IDPrefixes, MessageTexts
)


class TestStatusCode:
    """Test StatusCode enum"""
    
    def test_status_code_values(self):
        """Test that all status codes have correct values"""
        assert StatusCode.NEW.value == 'N'
        assert StatusCode.PROCESSED.value == 'P'
        assert StatusCode.ERROR.value == 'E'
        assert StatusCode.WARNING.value == 'W'
        assert StatusCode.SUCCESS.value == 'S'
        assert StatusCode.INFO.value == 'I'
    
    def test_status_code_enum_members(self):
        """Test that all expected status codes exist"""
        expected_members = {'NEW', 'PROCESSED', 'ERROR', 'WARNING', 'SUCCESS', 'INFO'}
        actual_members = {status.name for status in StatusCode}
        assert actual_members == expected_members


class TestProcessStep:
    """Test ProcessStep enum"""
    
    def test_process_step_values(self):
        """Test that all process steps have correct values"""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'
    
    def test_process_step_enum_members(self):
        """Test that all expected process steps exist"""
        expected_members = {
            'INIT', 'EXTRACT', 'TRANSFORM', 'LOAD', 
            'VALIDATE', 'COMPLETE', 'ERROR'
        }
        actual_members = {step.name for step in ProcessStep}
        assert actual_members == expected_members


class TestSaleCategory:
    """Test SaleCategory enum"""
    
    def test_sale_category_values(self):
        """Test that all sale categories have correct values"""
        assert SaleCategory.HIGH.value == 'HIGH'
        assert SaleCategory.MEDIUM.value == 'MEDIUM'
        assert SaleCategory.LOW.value == 'LOW'
    
    def test_sale_category_enum_members(self):
        """Test that all expected categories exist"""
        expected_members = {'HIGH', 'MEDIUM', 'LOW'}
        actual_members = {cat.name for cat in SaleCategory}
        assert actual_members == expected_members


class TestBusinessRules:
    """Test BusinessRules dataclass"""
    
    def test_discount_thresholds(self):
        """Test discount quantity thresholds"""
        rules = BusinessRules()
        assert rules.DISCOUNT_QTY_TIER1 == 10
        assert rules.DISCOUNT_QTY_TIER2 == 15
    
    def test_discount_rates(self):
        """Test discount rates are correct Decimal values"""
        rules = BusinessRules()
        assert rules.DISCOUNT_RATE_TIER1 == Decimal('0.05')
        assert rules.DISCOUNT_RATE_TIER2 == Decimal('0.10')
    
    def test_tax_rate(self):
        """Test tax rate"""
        rules = BusinessRules()
        assert rules.TAX_RATE == Decimal('0.08')
    
    def test_cost_ratio(self):
        """Test cost ratio"""
        rules = BusinessRules()
        assert rules.COST_RATIO == Decimal('0.60')
    
    def test_category_thresholds(self):
        """Test category thresholds"""
        rules = BusinessRules()
        assert rules.CATEGORY_HIGH_THRESHOLD == Decimal('2000.00')
        assert rules.CATEGORY_MEDIUM_THRESHOLD == Decimal('500.00')
    
    def test_immutability(self):
        """Test that BusinessRules is immutable"""
        rules = BusinessRules()
        with pytest.raises(Exception):  # FrozenInstanceError
            rules.TAX_RATE = Decimal('0.10')


class TestETLConfig:
    """Test ETLConfig dataclass"""
    
    def test_default_values(self):
        """Test all default configuration values"""
        config = ETLConfig()
        assert config.DEFAULT_BATCH_SIZE == 1000
        assert config.DEFAULT_COMMIT_INTERVAL == 500
        assert config.DEFAULT_RETRY_ATTEMPTS == 3
        assert config.DEFAULT_TIMEOUT_SECONDS == 3600
    
    def test_immutability(self):
        """Test that ETLConfig is immutable"""
        config = ETLConfig()
        with pytest.raises(Exception):  # FrozenInstanceError
            config.DEFAULT_BATCH_SIZE = 2000


class TestIDPrefixes:
    """Test IDPrefixes dataclass"""
    
    def test_prefix_values(self):
        """Test all ID prefix values"""
        prefixes = IDPrefixes()
        assert prefixes.ETL_RUN == 'ETL'
        assert prefixes.LOG_ID == 'LOG'
        assert prefixes.ANALYTICS_ID == 'ANL'
    
    def test_immutability(self):
        """Test that IDPrefixes is immutable"""
        prefixes = IDPrefixes()
        with pytest.raises(Exception):  # FrozenInstanceError
            prefixes.ETL_RUN = 'NEW'


class TestMessageTexts:
    """Test MessageTexts dataclass"""
    
    def test_message_values(self):
        """Test that all message texts are defined"""
        messages = MessageTexts()
        assert messages.INIT_SUCCESS == 'ETL process initialized successfully'
        assert messages.EXTRACT_START == 'Starting data extraction'
        assert messages.EXTRACT_COMPLETE == 'Data extraction completed'
        assert messages.TRANSFORM_START == 'Starting data transformation'
        assert messages.TRANSFORM_COMPLETE == 'Data transformation completed'
        assert messages.LOAD_START == 'Starting data load'
        assert messages.LOAD_COMPLETE == 'Data load completed'
        assert messages.ETL_COMPLETE == 'ETL process completed successfully'
        assert messages.ETL_ERROR == 'ETL process failed'
    
    def test_message_non_empty(self):
        """Test that all messages are non-empty strings"""
        messages = MessageTexts()
        for attr_name in dir(messages):
            if not attr_name.startswith('_'):
                message = getattr(messages, attr_name)
                assert isinstance(message, str)
                assert len(message) > 0


class TestETLConstants:
    """Test ETLConstants main class"""
    
    def test_constant_attributes(self):
        """Test that all constant attributes are accessible"""
        assert hasattr(ETLConstants, 'STATUS')
        assert hasattr(ETLConstants, 'STEP')
        assert hasattr(ETLConstants, 'CATEGORY')
        assert hasattr(ETLConstants, 'BUSINESS_RULES')
        assert hasattr(ETLConstants, 'CONFIG')
        assert hasattr(ETLConstants, 'PREFIXES')
        assert hasattr(ETLConstants, 'MESSAGES')
    
    def test_status_access(self):
        """Test accessing status codes through ETLConstants"""
        assert ETLConstants.STATUS.NEW.value == 'N'
        assert ETLConstants.STATUS.SUCCESS.value == 'S'
    
    def test_step_access(self):
        """Test accessing process steps through ETLConstants"""
        assert ETLConstants.STEP.EXTRACT.value == 'EXTRACT'
        assert ETLConstants.STEP.TRANSFORM.value == 'TRANSFORM'
    
    def test_category_access(self):
        """Test accessing categories through ETLConstants"""
        assert ETLConstants.CATEGORY.HIGH.value == 'HIGH'
        assert ETLConstants.CATEGORY.LOW.value == 'LOW'
    
    def test_get_discount_rate_no_discount(self):
        """Test discount rate calculation for low quantities"""
        rate = ETLConstants.get_discount_rate(5)
        assert rate == Decimal('0.00')
    
    def test_get_discount_rate_tier1(self):
        """Test discount rate calculation for tier 1"""
        rate = ETLConstants.get_discount_rate(12)
        assert rate == Decimal('0.05')
    
    def test_get_discount_rate_tier2(self):
        """Test discount rate calculation for tier 2"""
        rate = ETLConstants.get_discount_rate(20)
        assert rate == Decimal('0.10')
    
    def test_get_discount_rate_boundary_tier1(self):
        """Test discount rate at tier 1 boundary"""
        rate_below = ETLConstants.get_discount_rate(10)
        rate_above = ETLConstants.get_discount_rate(11)
        assert rate_below == Decimal('0.00')
        assert rate_above == Decimal('0.05')
    
    def test_get_discount_rate_boundary_tier2(self):
        """Test discount rate at tier 2 boundary"""
        rate_below = ETLConstants.get_discount_rate(15)
        rate_above = ETLConstants.get_discount_rate(16)
        assert rate_below == Decimal('0.05')
        assert rate_above == Decimal('0.10')
    
    def test_categorize_sale_low(self):
        """Test sale categorization for low amounts"""
        category = ETLConstants.categorize_sale(Decimal('100.00'))
        assert category == SaleCategory.LOW
    
    def test_categorize_sale_medium(self):
        """Test sale categorization for medium amounts"""
        category = ETLConstants.categorize_sale(Decimal('1000.00'))
        assert category == SaleCategory.MEDIUM
    
    def test_categorize_sale_high(self):
        """Test sale categorization for high amounts"""
        category = ETLConstants.categorize_sale(Decimal('3000.00'))
        assert category == SaleCategory.HIGH
    
    def test_categorize_sale_boundary_medium(self):
        """Test categorization at medium threshold boundary"""
        cat_below = ETLConstants.categorize_sale(Decimal('499.99'))
        cat_exact = ETLConstants.categorize_sale(Decimal('500.00'))
        cat_above = ETLConstants.categorize_sale(Decimal('500.01'))
        assert cat_below == SaleCategory.LOW
        assert cat_exact == SaleCategory.MEDIUM
        assert cat_above == SaleCategory.MEDIUM
    
    def test_categorize_sale_boundary_high(self):
        """Test categorization at high threshold boundary"""
        cat_below = ETLConstants.categorize_sale(Decimal('1999.99'))
        cat_exact = ETLConstants.categorize_sale(Decimal('2000.00'))
        cat_above = ETLConstants.categorize_sale(Decimal('2000.01'))
        assert cat_below == SaleCategory.MEDIUM
        assert cat_exact == SaleCategory.HIGH
        assert cat_above == SaleCategory.HIGH
    
    def test_to_dict_structure(self):
        """Test that to_dict returns proper structure"""
        config_dict = ETLConstants.to_dict()
        
        # Check top-level keys
        assert 'status_codes' in config_dict
        assert 'process_steps' in config_dict
        assert 'sale_categories' in config_dict
        assert 'business_rules' in config_dict
        assert 'etl_config' in config_dict
        assert 'prefixes' in config_dict
        assert 'messages' in config_dict
    
    def test_to_dict_status_codes(self):
        """Test status codes in dictionary output"""
        config_dict = ETLConstants.to_dict()
        status_codes = config_dict['status_codes']
        
        assert status_codes['NEW'] == 'N'
        assert status_codes['PROCESSED'] == 'P'
        assert status_codes['ERROR'] == 'E'
    
    def test_to_dict_business_rules(self):
        """Test business rules in dictionary output"""
        config_dict = ETLConstants.to_dict()
        rules = config_dict['business_rules']
        
        assert rules['discount_qty_tier1'] == 10
        assert rules['discount_qty_tier2'] == 15
        assert rules['discount_rate_tier1'] == '0.05'
        assert rules['tax_rate'] == '0.08'
    
    def test_to_dict_etl_config(self):
        """Test ETL config in dictionary output"""
        config_dict = ETLConstants.to_dict()
        etl_config = config_dict['etl_config']
        
        assert etl_config['default_batch_size'] == 1000
        assert etl_config['default_commit_interval'] == 500
        assert etl_config['default_retry_attempts'] == 3


class TestBackwardCompatibility:
    """Test backward compatibility aliases"""
    
    def test_gc_status_alias(self):
        """Test GC_STATUS alias works"""
        from src.config import GC_STATUS
        assert GC_STATUS == StatusCode
        assert GC_STATUS.NEW.value == 'N'
    
    def test_gc_step_alias(self):
        """Test GC_STEP alias works"""
        from src.config import GC_STEP
        assert GC_STEP == ProcessStep
        assert GC_STEP.EXTRACT.value == 'EXTRACT'
    
    def test_gc_category_alias(self):
        """Test GC_CATEGORY alias works"""
        from src.config import GC_CATEGORY
        assert GC_CATEGORY == SaleCategory
        assert GC_CATEGORY.HIGH.value == 'HIGH'


class TestDecimalPrecision:
    """Test decimal precision for financial calculations"""
    
    def test_discount_rate_precision(self):
        """Test that discount rates maintain precision"""
        rules = BusinessRules()
        rate1 = rules.DISCOUNT_RATE_TIER1
        rate2 = rules.DISCOUNT_RATE_TIER2
        
        assert str(rate1) == '0.05'
        assert str(rate2) == '0.10'
    
    def test_tax_rate_precision(self):
        """Test that tax rate maintains precision"""
        rules = BusinessRules()
        assert str(rules.TAX_RATE) == '0.08'
    
    def test_threshold_precision(self):
        """Test that thresholds maintain precision"""
        rules = BusinessRules()
        assert str(rules.CATEGORY_HIGH_THRESHOLD) == '2000.00'
        assert str(rules.CATEGORY_MEDIUM_THRESHOLD) == '500.00'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])