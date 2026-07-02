"""
Unit tests for type definitions and dataclasses.
"""

import pytest
from datetime import date, datetime
from decimal import Decimal
from src.common.types import (
    RawSales, Analytics, ETLLog, ETLConfig, 
    ETLStatistics, StatusCode, ProcessStep, SaleCategory
)


class TestEnums:
    """Test enum definitions."""
    
    def test_status_code_values(self):
        """Test StatusCode enum values."""
        assert StatusCode.NEW == "N"
        assert StatusCode.PROCESSED == "P"
        assert StatusCode.ERROR == "E"
        assert StatusCode.SUCCESS == "S"
    
    def test_process_step_values(self):
        """Test ProcessStep enum values."""
        assert ProcessStep.INIT == "INIT"
        assert ProcessStep.EXTRACT == "EXTRACT"
        assert ProcessStep.TRANSFORM == "TRANSFORM"
        assert ProcessStep.LOAD == "LOAD"
    
    def test_sale_category_values(self):
        """Test SaleCategory enum values."""
        assert SaleCategory.HIGH == "HIGH"
        assert SaleCategory.MEDIUM == "MEDIUM"
        assert SaleCategory.LOW == "LOW"


class TestRawSales:
    """Test RawSales dataclass."""
    
    def test_raw_sales_creation(self):
        """Test creating RawSales instance."""
        raw_sale = RawSales(
            trans_id="T000001",
            trans_date=date(2024, 1, 15),
            customer_id="CUST001",
            product_id="PROD001",
            quantity=10,
            unit_price=Decimal("99.99"),
            currency="USD",
            sales_rep="John Doe",
            region="NORTH",
            status="N"
        )
        
        assert raw_sale.trans_id == "T000001"
        assert raw_sale.quantity == 10
        assert raw_sale.unit_price == Decimal("99.99")
        assert raw_sale.created_at is None  # Optional field
    
    def test_raw_sales_with_optional_fields(self):
        """Test RawSales with optional fields."""
        now = datetime.now()
        raw_sale = RawSales(
            trans_id="T000001",
            trans_date=date(2024, 1, 15),
            customer_id="CUST001",
            product_id="PROD001",
            quantity=10,
            unit_price=Decimal("99.99"),
            currency="USD",
            sales_rep="John Doe",
            region="NORTH",
            status="N",
            created_at=now,
            created_by="TEST_USER"
        )
        
        assert raw_sale.created_at == now
        assert raw_sale.created_by == "TEST_USER"


class TestAnalytics:
    """Test Analytics dataclass."""
    
    def test_analytics_creation(self):
        """Test creating Analytics instance."""
        analytics = Analytics(
            analytics_id="ANL20240115120000",
            trans_date=date(2024, 1, 15),
            customer_id="CUST001",
            product_id="PROD001",
            total_quantity=10,
            gross_amount=Decimal("999.90"),
            net_amount=Decimal("1029.89"),
            discount_amount=Decimal("49.99"),
            tax_amount=Decimal("79.99"),
            currency="USD",
            sales_rep="John Doe",
            region="NORTH",
            profit_margin=Decimal("38.35"),
            category="MEDIUM",
            etl_run_id="ETL20240115120000"
        )
        
        assert analytics.analytics_id == "ANL20240115120000"
        assert analytics.total_quantity == 10
        assert analytics.gross_amount == Decimal("999.90")
        assert analytics.category == "MEDIUM"


class TestETLLog:
    """Test ETLLog dataclass."""
    
    def test_etl_log_creation(self):
        """Test creating ETLLog instance."""
        log = ETLLog(
            log_id="LOG20240115120000",
            etl_run_id="ETL20240115120000",
            execution_date=date(2024, 1, 15),
            execution_time="12:00:00",
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=100,
            records_error=0,
            message="Extraction completed successfully"
        )
        
        assert log.log_id == "LOG20240115120000"
        assert log.process_step == "EXTRACT"
        assert log.records_processed == 100
        assert log.status == "S"


class TestETLConfig:
    """Test ETLConfig dataclass."""
    
    def test_etl_config_defaults(self):
        """Test ETLConfig default values."""
        config = ETLConfig()
        
        assert config.batch_size == 1000
        assert config.commit_interval == 500
        assert config.parallel_jobs == 4
        assert config.retry_attempts == 3
        assert config.timeout_seconds == 3600
    
    def test_etl_config_custom_values(self):
        """Test ETLConfig with custom values."""
        config = ETLConfig(
            batch_size=500,
            parallel_jobs=8,
            retry_attempts=5
        )
        
        assert config.batch_size == 500
        assert config.parallel_jobs == 8
        assert config.retry_attempts == 5


class TestETLStatistics:
    """Test ETLStatistics dataclass."""
    
    def test_etl_statistics_defaults(self):
        """Test ETLStatistics default values."""
        stats = ETLStatistics()
        
        assert stats.total_records == 0
        assert stats.success_records == 0
        assert stats.error_records == 0
        assert stats.warning_records == 0
        assert stats.start_time is None
    
    def test_etl_statistics_with_values(self):
        """Test ETLStatistics with values."""
        start = datetime.now()
        stats = ETLStatistics(
            total_records=1000,
            success_records=980,
            error_records=20,
            start_time=start
        )
        
        assert stats.total_records == 1000
        assert stats.success_records == 980
        assert stats.error_records == 20
        assert stats.start_time == start