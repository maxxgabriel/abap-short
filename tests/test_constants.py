"""
Unit tests for ETL constants.
"""

import pytest
from decimal import Decimal
from src.common.constants import ETLConstants


class TestETLConstants:
    """Test ETL constants."""
    
    def test_status_codes(self):
        """Test status code constants."""
        assert ETLConstants.Status.NEW == "N"
        assert ETLConstants.Status.PROCESSED == "P"
        assert ETLConstants.Status.ERROR == "E"
        assert ETLConstants.Status.SUCCESS == "S"
    
    def test_process_steps(self):
        """Test process step constants."""
        assert ETLConstants.Step.INIT == "INIT"
        assert ETLConstants.Step.EXTRACT == "EXTRACT"
        assert ETLConstants.Step.TRANSFORM == "TRANSFORM"
        assert ETLConstants.Step.LOAD == "LOAD"
    
    def test_categories(self):
        """Test category constants."""
        assert ETLConstants.Category.HIGH == "HIGH"
        assert ETLConstants.Category.MEDIUM == "MEDIUM"
        assert ETLConstants.Category.LOW == "LOW"
    
    def test_discount_thresholds(self):
        """Test discount threshold constants."""
        assert ETLConstants.DISCOUNT_QTY_TIER1 == 10
        assert ETLConstants.DISCOUNT_QTY_TIER2 == 15
        assert ETLConstants.DISCOUNT_RATE_TIER1 == Decimal("0.05")
        assert ETLConstants.DISCOUNT_RATE_TIER2 == Decimal("0.10")
    
    def test_tax_rate(self):
        """Test tax rate constant."""
        assert ETLConstants.TAX_RATE == Decimal("0.08")
    
    def test_cost_ratio(self):
        """Test cost ratio constant."""
        assert ETLConstants.COST_RATIO == Decimal("0.60")
    
    def test_category_thresholds(self):
        """Test category threshold constants."""
        assert ETLConstants.CATEGORY_HIGH_THRESHOLD == Decimal("2000.00")
        assert ETLConstants.CATEGORY_MEDIUM_THRESHOLD == Decimal("500.00")
    
    def test_default_config_values(self):
        """Test default configuration constants."""
        assert ETLConstants.DEFAULT_BATCH_SIZE == 1000
        assert ETLConstants.DEFAULT_COMMIT_INTERVAL == 500
        assert ETLConstants.DEFAULT_RETRY_ATTEMPTS == 3
        assert ETLConstants.DEFAULT_TIMEOUT_SECONDS == 3600
    
    def test_id_prefixes(self):
        """Test ID prefix constants."""
        assert ETLConstants.PREFIX_ETL_RUN == "ETL"
        assert ETLConstants.PREFIX_LOG_ID == "LOG"
        assert ETLConstants.PREFIX_ANALYTICS_ID == "ANL"
    
    def test_message_texts(self):
        """Test message text constants."""
        assert "initialized successfully" in ETLConstants.MSG_INIT_SUCCESS
        assert "extraction" in ETLConstants.MSG_EXTRACT_START.lower()
        assert "completed" in ETLConstants.MSG_ETL_COMPLETE.lower()