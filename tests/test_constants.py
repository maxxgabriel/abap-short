"""
Unit Tests for ETL Constants Module
"""

import pytest
from decimal import Decimal
from src.constants import (
    Status,
    ProcessStep,
    Category,
    BusinessRules,
    ETLConfig,
    IDPrefixes,
    Messages
)


class TestStatusEnum:
    """Test Status enum."""
    
    def test_status_values(self):
        """Test all status values."""
        assert Status.NEW.value == 'N'
        assert Status.PROCESSED.value == 'P'
        assert Status.ERROR.value == 'E'
        assert Status.WARNING.value == 'W'
        assert Status.SUCCESS.value == 'S'
        assert Status.INFO.value == 'I'


class TestProcessStepEnum:
    """Test ProcessStep enum."""
    
    def test_step_values(self):
        """Test all process step values."""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'


class TestCategoryEnum:
    """Test Category enum."""
    
    def test_category_values(self):
        """Test all category values."""
        assert Category.HIGH.value == 'HIGH'
        assert Category.MEDIUM.value == 'MEDIUM'
        assert Category.LOW.value == 'LOW'


class TestBusinessRules:
    """Test BusinessRules constants."""
    
    def test_discount_thresholds(self):
        """Test discount tier thresholds."""
        assert BusinessRules.DISCOUNT_QTY_TIER1 == 10
        assert BusinessRules.DISCOUNT_QTY_TIER2 == 15
    
    def test_discount_rates(self):
        """Test discount rates."""
        assert BusinessRules.DISCOUNT_RATE_TIER1 == Decimal('0.05')
        assert BusinessRules.DISCOUNT_RATE_TIER2 == Decimal('0.10')
    
    def test_tax_rate(self):
        """Test tax rate."""
        assert BusinessRules.TAX_RATE == Decimal('0.08')
    
    def test_cost_ratio(self):
        """Test cost ratio."""
        assert BusinessRules.COST_RATIO == Decimal('0.60')
    
    def test_category_thresholds(self):
        """Test category thresholds."""
        assert BusinessRules.CATEGORY_HIGH_THRESHOLD == Decimal('2000.00')
        assert BusinessRules.CATEGORY_MEDIUM_THRESHOLD == Decimal('500.00')
    
    def test_decimal_precision(self):
        """Test decimal types have correct precision."""
        # Discount rates should be 2 decimal places
        rate1_str = str(BusinessRules.DISCOUNT_RATE_TIER1)
        rate2_str = str(BusinessRules.DISCOUNT_RATE_TIER2)
        assert '0.05' in rate1_str
        assert '0.10' in rate2_str or '0.1' in rate2_str


class TestETLConfig:
    """Test ETLConfig constants."""
    
    def test_batch_size(self):
        """Test batch size configuration."""
        assert ETLConfig.DEFAULT_BATCH_SIZE == 1000
    
    def test_commit_interval(self):
        """Test commit interval configuration."""
        assert ETLConfig.DEFAULT_COMMIT_INTERVAL == 500
    
    def test_retry_attempts(self):
        """Test retry attempts configuration."""
        assert ETLConfig.DEFAULT_RETRY_ATTEMPTS == 3
    
    def test_timeout_seconds(self):
        """Test timeout configuration."""
        assert ETLConfig.DEFAULT_TIMEOUT_SECONDS == 3600


class TestIDPrefixes:
    """Test ID prefix constants."""
    
    def test_etl_run_prefix(self):
        """Test ETL run prefix."""
        assert IDPrefixes.ETL_RUN == 'ETL'
    
    def test_log_id_prefix(self):
        """Test log ID prefix."""
        assert IDPrefixes.LOG_ID == 'LOG'
    
    def test_analytics_id_prefix(self):
        """Test analytics ID prefix."""
        assert IDPrefixes.ANALYTICS_ID == 'ANL'


class TestMessages:
    """Test message constants."""
    
    def test_init_message(self):
        """Test initialization message."""
        assert Messages.INIT_SUCCESS == 'ETL process initialized successfully'
    
    def test_extract_messages(self):
        """Test extraction messages."""
        assert Messages.EXTRACT_START == 'Starting data extraction'
        assert Messages.EXTRACT_COMPLETE == 'Data extraction completed'
    
    def test_transform_messages(self):
        """Test transformation messages."""
        assert Messages.TRANSFORM_START == 'Starting data transformation'
        assert Messages.TRANSFORM_COMPLETE == 'Data transformation completed'
    
    def test_load_messages(self):
        """Test load messages."""
        assert Messages.LOAD_START == 'Starting data load'
        assert Messages.LOAD_COMPLETE == 'Data load completed'
    
    def test_completion_messages(self):
        """Test completion messages."""
        assert Messages.ETL_COMPLETE == 'ETL process completed successfully'
        assert Messages.ETL_ERROR == 'ETL process failed'


class TestBusinessRulesApplication:
    """Test business rules in realistic scenarios."""
    
    def test_discount_tier1_threshold(self):
        """Test tier 1 discount threshold."""
        quantity = 11
        assert quantity > BusinessRules.DISCOUNT_QTY_TIER1
        assert quantity <= BusinessRules.DISCOUNT_QTY_TIER2
    
    def test_discount_tier2_threshold(self):
        """Test tier 2 discount threshold."""
        quantity = 20
        assert quantity > BusinessRules.DISCOUNT_QTY_TIER2
    
    def test_category_high(self):
        """Test high category threshold."""
        amount = Decimal('2500.00')
        assert amount >= BusinessRules.CATEGORY_HIGH_THRESHOLD
    
    def test_category_medium(self):
        """Test medium category threshold."""
        amount = Decimal('1000.00')
        assert amount >= BusinessRules.CATEGORY_MEDIUM_THRESHOLD
        assert amount < BusinessRules.CATEGORY_HIGH_THRESHOLD
    
    def test_category_low(self):
        """Test low category threshold."""
        amount = Decimal('300.00')
        assert amount < BusinessRules.CATEGORY_MEDIUM_THRESHOLD
    
    def test_tax_calculation(self):
        """Test tax calculation."""
        gross = Decimal('1000.00')
        tax = gross * BusinessRules.TAX_RATE
        assert tax == Decimal('80.00')
    
    def test_cost_calculation(self):
        """Test cost calculation."""
        price = Decimal('100.00')
        cost = price * BusinessRules.COST_RATIO
        assert cost == Decimal('60.00')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])