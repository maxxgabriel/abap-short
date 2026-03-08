"""
Unit tests for ETL Constants and Configuration
"""
import pytest
from decimal import Decimal
from src.constants import (
    StatusCode,
    ProcessStep,
    SaleCategory,
    ETLConstants,
    IDGenerator
)


class TestStatusCode:
    """Test StatusCode enum"""
    
    def test_status_codes_defined(self):
        """Test all status codes are defined"""
        assert StatusCode.NEW == 'N'
        assert StatusCode.PROCESSED == 'P'
        assert StatusCode.ERROR == 'E'
        assert StatusCode.WARNING == 'W'
        assert StatusCode.SUCCESS == 'S'
        assert StatusCode.INFO == 'I'
    
    def test_status_code_enum_values(self):
        """Test status code enum values"""
        assert len(list(StatusCode)) == 6
        assert 'N' in [s.value for s in StatusCode]


class TestProcessStep:
    """Test ProcessStep enum"""
    
    def test_process_steps_defined(self):
        """Test all process steps are defined"""
        assert ProcessStep.INIT == 'INIT'
        assert ProcessStep.EXTRACT == 'EXTRACT'
        assert ProcessStep.TRANSFORM == 'TRANSFORM'
        assert ProcessStep.LOAD == 'LOAD'
        assert ProcessStep.VALIDATE == 'VALIDATE'
        assert ProcessStep.COMPLETE == 'COMPLETE'
        assert ProcessStep.ERROR == 'ERROR'
    
    def test_process_step_enum_values(self):
        """Test process step enum values"""
        assert len(list(ProcessStep)) == 7


class TestSaleCategory:
    """Test SaleCategory enum"""
    
    def test_sale_categories_defined(self):
        """Test all sale categories are defined"""
        assert SaleCategory.HIGH == 'HIGH'
        assert SaleCategory.MEDIUM == 'MEDIUM'
        assert SaleCategory.LOW == 'LOW'
    
    def test_sale_category_enum_values(self):
        """Test sale category enum values"""
        assert len(list(SaleCategory)) == 3


class TestETLConstants:
    """Test ETL Constants"""
    
    def test_discount_thresholds(self):
        """Test discount threshold constants"""
        assert ETLConstants.DISCOUNT_QTY_TIER1 == 10
        assert ETLConstants.DISCOUNT_QTY_TIER2 == 15
        assert ETLConstants.DISCOUNT_RATE_TIER1 == Decimal('0.05')
        assert ETLConstants.DISCOUNT_RATE_TIER2 == Decimal('0.10')
    
    def test_business_rules(self):
        """Test business rule constants"""
        assert ETLConstants.TAX_RATE == Decimal('0.08')
        assert ETLConstants.COST_RATIO == Decimal('0.60')
        assert ETLConstants.CATEGORY_HIGH_THRESHOLD == Decimal('2000.00')
        assert ETLConstants.CATEGORY_MEDIUM_THRESHOLD == Decimal('500.00')
    
    def test_etl_configuration_defaults(self):
        """Test ETL configuration default values"""
        assert ETLConstants.DEFAULT_BATCH_SIZE == 1000
        assert ETLConstants.DEFAULT_COMMIT_INTERVAL == 500
        assert ETLConstants.DEFAULT_RETRY_ATTEMPTS == 3
        assert ETLConstants.DEFAULT_TIMEOUT_SECONDS == 3600
    
    def test_id_prefixes(self):
        """Test ID prefix constants"""
        assert ETLConstants.PREFIX_ETL_RUN == 'ETL'
        assert ETLConstants.PREFIX_LOG_ID == 'LOG'
        assert ETLConstants.PREFIX_ANALYTICS_ID == 'ANL'
    
    def test_message_templates(self):
        """Test message template constants"""
        assert ETLConstants.MSG_INIT_SUCCESS == 'ETL process initialized successfully'
        assert ETLConstants.MSG_EXTRACT_START == 'Starting data extraction'
        assert ETLConstants.MSG_ETL_COMPLETE == 'ETL process completed successfully'
        assert isinstance(ETLConstants.MSG_ETL_ERROR, str)


class TestIDGenerator:
    """Test ID Generator"""
    
    def test_generate_etl_run_id(self):
        """Test ETL run ID generation"""
        run_id = IDGenerator.generate_etl_run_id()
        assert run_id.startswith('ETL')
        assert len(run_id) > 3
        assert run_id[3:].isdigit()
    
    def test_generate_log_id(self):
        """Test log ID generation"""
        log_id = IDGenerator.generate_log_id()
        assert log_id.startswith('LOG')
        assert len(log_id) > 3
        assert log_id[3:].isdigit()
    
    def test_generate_analytics_id(self):
        """Test analytics ID generation"""
        trans_id = 'T000001'
        analytics_id = IDGenerator.generate_analytics_id(trans_id)
        assert analytics_id.startswith('ANL')
        assert trans_id in analytics_id
        assert len(analytics_id) > len('ANL' + trans_id)
    
    def test_unique_ids(self):
        """Test that generated IDs are unique"""
        import time
        id1 = IDGenerator.generate_etl_run_id()
        time.sleep(0.01)  # Small delay to ensure different timestamp
        id2 = IDGenerator.generate_etl_run_id()
        assert id1 != id2


class TestDecimalPrecision:
    """Test Decimal precision for business rules"""
    
    def test_discount_rate_precision(self):
        """Test discount rate decimal precision"""
        rate1 = ETLConstants.DISCOUNT_RATE_TIER1
        rate2 = ETLConstants.DISCOUNT_RATE_TIER2
        
        assert isinstance(rate1, Decimal)
        assert isinstance(rate2, Decimal)
        assert rate1 == Decimal('0.05')
        assert rate2 == Decimal('0.10')
    
    def test_tax_rate_precision(self):
        """Test tax rate decimal precision"""
        tax_rate = ETLConstants.TAX_RATE
        assert isinstance(tax_rate, Decimal)
        assert tax_rate == Decimal('0.08')
    
    def test_threshold_precision(self):
        """Test threshold decimal precision"""
        high_threshold = ETLConstants.CATEGORY_HIGH_THRESHOLD
        medium_threshold = ETLConstants.CATEGORY_MEDIUM_THRESHOLD
        
        assert isinstance(high_threshold, Decimal)
        assert isinstance(medium_threshold, Decimal)
        assert high_threshold == Decimal('2000.00')
        assert medium_threshold == Decimal('500.00')