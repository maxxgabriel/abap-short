"""
Unit Tests for ETL Constants Module

Tests the constants and configuration values defined in the constants module.
"""

import pytest
from src.constants import (
    ETLConstants,
    StatusCodes,
    ProcessSteps,
    SaleCategories,
    BusinessRules,
    ETLDefaults,
    IDPrefixes,
    Messages
)


class TestStatusCodes:
    """Test status code constants"""
    
    def test_status_codes_defined(self):
        """Test all status codes are defined"""
        status = ETLConstants.STATUS
        assert status.NEW == 'N'
        assert status.PROCESSED == 'P'
        assert status.ERROR == 'E'
        assert status.WARNING == 'W'
        assert status.SUCCESS == 'S'
        assert status.INFO == 'I'
    
    def test_status_codes_immutable(self):
        """Test status codes are immutable"""
        status = ETLConstants.STATUS
        with pytest.raises(AttributeError):
            status.NEW = 'X'


class TestProcessSteps:
    """Test process step constants"""
    
    def test_process_steps_defined(self):
        """Test all process steps are defined"""
        steps = ETLConstants.STEP
        assert steps.INIT == 'INIT'
        assert steps.EXTRACT == 'EXTRACT'
        assert steps.TRANSFORM == 'TRANSFORM'
        assert steps.LOAD == 'LOAD'
        assert steps.VALIDATE == 'VALIDATE'
        assert steps.COMPLETE == 'COMPLETE'
        assert steps.ERROR == 'ERROR'
    
    def test_steps_unique(self):
        """Test all steps have unique values"""
        steps = ETLConstants.STEP
        step_values = [
            steps.INIT, steps.EXTRACT, steps.TRANSFORM,
            steps.LOAD, steps.VALIDATE, steps.COMPLETE, steps.ERROR
        ]
        assert len(step_values) == len(set(step_values))


class TestSaleCategories:
    """Test sale category constants"""
    
    def test_categories_defined(self):
        """Test all categories are defined"""
        categories = ETLConstants.CATEGORY
        assert categories.HIGH == 'HIGH'
        assert categories.MEDIUM == 'MEDIUM'
        assert categories.LOW == 'LOW'
    
    def test_categories_valid(self):
        """Test categories are valid strings"""
        categories = ETLConstants.CATEGORY
        for cat in [categories.HIGH, categories.MEDIUM, categories.LOW]:
            assert isinstance(cat, str)
            assert len(cat) > 0


class TestBusinessRules:
    """Test business rules constants"""
    
    def test_discount_thresholds(self):
        """Test discount quantity thresholds"""
        rules = ETLConstants.RULES
        assert rules.DISCOUNT_QTY_TIER1 == 10
        assert rules.DISCOUNT_QTY_TIER2 == 15
        assert rules.DISCOUNT_QTY_TIER2 > rules.DISCOUNT_QTY_TIER1
    
    def test_discount_rates(self):
        """Test discount rates are valid percentages"""
        rules = ETLConstants.RULES
        assert 0 <= rules.DISCOUNT_RATE_TIER1 <= 1
        assert 0 <= rules.DISCOUNT_RATE_TIER2 <= 1
        assert rules.DISCOUNT_RATE_TIER2 > rules.DISCOUNT_RATE_TIER1
    
    def test_tax_rate(self):
        """Test tax rate is valid"""
        rules = ETLConstants.RULES
        assert 0 <= rules.TAX_RATE <= 1
        assert rules.TAX_RATE == 0.08
    
    def test_cost_ratio(self):
        """Test cost ratio is valid"""
        rules = ETLConstants.RULES
        assert 0 <= rules.COST_RATIO <= 1
        assert rules.COST_RATIO == 0.60
    
    def test_category_thresholds(self):
        """Test category thresholds are properly ordered"""
        rules = ETLConstants.RULES
        assert rules.CATEGORY_HIGH_THRESHOLD > rules.CATEGORY_MEDIUM_THRESHOLD
        assert rules.CATEGORY_MEDIUM_THRESHOLD > 0
        assert rules.CATEGORY_HIGH_THRESHOLD == 2000.00
        assert rules.CATEGORY_MEDIUM_THRESHOLD == 500.00


class TestETLDefaults:
    """Test ETL default configuration"""
    
    def test_defaults_positive(self):
        """Test all defaults are positive integers"""
        defaults = ETLConstants.DEFAULTS
        assert defaults.BATCH_SIZE > 0
        assert defaults.COMMIT_INTERVAL > 0
        assert defaults.RETRY_ATTEMPTS >= 0
        assert defaults.TIMEOUT_SECONDS > 0
    
    def test_default_values(self):
        """Test default values match specification"""
        defaults = ETLConstants.DEFAULTS
        assert defaults.BATCH_SIZE == 1000
        assert defaults.COMMIT_INTERVAL == 500
        assert defaults.RETRY_ATTEMPTS == 3
        assert defaults.TIMEOUT_SECONDS == 3600
    
    def test_commit_interval_less_than_batch(self):
        """Test commit interval is less than batch size"""
        defaults = ETLConstants.DEFAULTS
        assert defaults.COMMIT_INTERVAL <= defaults.BATCH_SIZE


class TestIDPrefixes:
    """Test ID prefix constants"""
    
    def test_prefixes_defined(self):
        """Test all prefixes are defined"""
        prefixes = ETLConstants.PREFIX
        assert prefixes.ETL_RUN == 'ETL'
        assert prefixes.LOG_ID == 'LOG'
        assert prefixes.ANALYTICS_ID == 'ANL'
    
    def test_prefixes_unique(self):
        """Test all prefixes are unique"""
        prefixes = ETLConstants.PREFIX
        prefix_values = [prefixes.ETL_RUN, prefixes.LOG_ID, prefixes.ANALYTICS_ID]
        assert len(prefix_values) == len(set(prefix_values))
    
    def test_prefixes_format(self):
        """Test prefixes are uppercase alphanumeric"""
        prefixes = ETLConstants.PREFIX
        for prefix in [prefixes.ETL_RUN, prefixes.LOG_ID, prefixes.ANALYTICS_ID]:
            assert prefix.isupper()
            assert prefix.isalpha()


class TestMessages:
    """Test message constants"""
    
    def test_messages_defined(self):
        """Test all standard messages are defined"""
        messages = ETLConstants.MSG
        assert messages.INIT_SUCCESS
        assert messages.EXTRACT_START
        assert messages.EXTRACT_COMPLETE
        assert messages.TRANSFORM_START
        assert messages.TRANSFORM_COMPLETE
        assert messages.LOAD_START
        assert messages.LOAD_COMPLETE
        assert messages.ETL_COMPLETE
        assert messages.ETL_ERROR
    
    def test_messages_not_empty(self):
        """Test messages are not empty strings"""
        messages = ETLConstants.MSG
        msg_values = [
            messages.INIT_SUCCESS,
            messages.EXTRACT_START,
            messages.EXTRACT_COMPLETE,
            messages.TRANSFORM_START,
            messages.TRANSFORM_COMPLETE,
            messages.LOAD_START,
            messages.LOAD_COMPLETE,
            messages.ETL_COMPLETE,
            messages.ETL_ERROR
        ]
        for msg in msg_values:
            assert isinstance(msg, str)
            assert len(msg) > 0


class TestETLConstants:
    """Test main ETL constants container"""
    
    def test_all_categories_present(self):
        """Test all constant categories are accessible"""
        assert hasattr(ETLConstants, 'STATUS')
        assert hasattr(ETLConstants, 'STEP')
        assert hasattr(ETLConstants, 'CATEGORY')
        assert hasattr(ETLConstants, 'RULES')
        assert hasattr(ETLConstants, 'DEFAULTS')
        assert hasattr(ETLConstants, 'PREFIX')
        assert hasattr(ETLConstants, 'MSG')
    
    def test_get_all_constants(self):
        """Test get_all_constants method returns complete dictionary"""
        all_constants = ETLConstants.get_all_constants()
        
        assert 'status' in all_constants
        assert 'steps' in all_constants
        assert 'categories' in all_constants
        assert 'rules' in all_constants
        assert 'defaults' in all_constants
        assert 'prefixes' in all_constants
        assert 'messages' in all_constants
    
    def test_constants_consistency(self):
        """Test constants are internally consistent"""
        # Discount tiers should be ordered
        assert ETLConstants.RULES.DISCOUNT_QTY_TIER1 < ETLConstants.RULES.DISCOUNT_QTY_TIER2
        
        # Discount rates should be ordered
        assert ETLConstants.RULES.DISCOUNT_RATE_TIER1 < ETLConstants.RULES.DISCOUNT_RATE_TIER2
        
        # Category thresholds should be ordered
        assert ETLConstants.RULES.CATEGORY_MEDIUM_THRESHOLD < ETLConstants.RULES.CATEGORY_HIGH_THRESHOLD