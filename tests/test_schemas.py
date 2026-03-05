"""
Unit tests for PySpark schema definitions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)
from datetime import date, datetime
from decimal import Decimal

from src.schemas import ETLSchemas


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder
        .appName("TestSchemas")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestRawSalesSchema:
    """Test raw sales schema definition."""
    
    def test_schema_structure(self):
        """Test that raw sales schema has correct structure."""
        schema = ETLSchemas.raw_sales_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 12
        
        # Check required fields
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "trans_id", "trans_date", "customer_id", "product_id",
            "quantity", "unit_price", "currency", "sales_rep",
            "region", "status", "created_at", "created_by"
        ]
        assert field_names == expected_fields
    
    def test_field_types(self):
        """Test that fields have correct data types."""
        schema = ETLSchemas.raw_sales_schema()
        field_map = {field.name: field for field in schema.fields}
        
        assert isinstance(field_map["trans_id"].dataType, StringType)
        assert isinstance(field_map["trans_date"].dataType, DateType)
        assert isinstance(field_map["quantity"].dataType, IntegerType)
        assert isinstance(field_map["unit_price"].dataType, DecimalType)
        assert field_map["unit_price"].dataType.precision == 16
        assert field_map["unit_price"].dataType.scale == 2
    
    def test_nullable_constraints(self):
        """Test nullable constraints on fields."""
        schema = ETLSchemas.raw_sales_schema()
        field_map = {field.name: field for field in schema.fields}
        
        # Required fields
        assert field_map["trans_id"].nullable is False
        assert field_map["customer_id"].nullable is False
        assert field_map["quantity"].nullable is False
        
        # Optional fields
        assert field_map["sales_rep"].nullable is True
        assert field_map["created_by"].nullable is True
    
    def test_create_dataframe_with_schema(self, spark):
        """Test creating DataFrame with raw sales schema."""
        schema = ETLSchemas.raw_sales_schema()
        
        data = [
            (
                "T000001",
                date(2024, 1, 15),
                "CUST001",
                "PROD001",
                10,
                Decimal("99.99"),
                "USD",
                "John Doe",
                "NORTH",
                "N",
                datetime(2024, 1, 15, 10, 30, 0),
                "ETL_USER",
            )
        ]
        
        df = spark.createDataFrame(data, schema)
        
        assert df.count() == 1
        assert df.schema == schema
        
        row = df.first()
        assert row.trans_id == "T000001"
        assert row.quantity == 10
        assert row.unit_price == Decimal("99.99")


class TestAnalyticsSchema:
    """Test analytics schema definition."""
    
    def test_schema_structure(self):
        """Test that analytics schema has correct structure."""
        schema = ETLSchemas.analytics_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 17
        
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "analytics_id", "trans_date", "customer_id", "product_id",
            "total_quantity", "gross_amount", "net_amount", "discount_amount",
            "tax_amount", "currency", "sales_rep", "region",
            "profit_margin", "category", "etl_run_id", "loaded_at", "loaded_by"
        ]
        assert field_names == expected_fields
    
    def test_decimal_precision(self):
        """Test decimal field precision and scale."""
        schema = ETLSchemas.analytics_schema()
        field_map = {field.name: field for field in schema.fields}
        
        # Amount fields should be Decimal(16, 2)
        assert field_map["gross_amount"].dataType.precision == 16
        assert field_map["gross_amount"].dataType.scale == 2
        assert field_map["net_amount"].dataType.precision == 16
        assert field_map["net_amount"].dataType.scale == 2
        
        # Profit margin should be Decimal(5, 2)
        assert field_map["profit_margin"].dataType.precision == 5
        assert field_map["profit_margin"].dataType.scale == 2
    
    def test_create_analytics_dataframe(self, spark):
        """Test creating DataFrame with analytics schema."""
        schema = ETLSchemas.analytics_schema()
        
        data = [
            (
                "ANL001",
                date(2024, 1, 15),
                "CUST001",
                "PROD001",
                10,
                Decimal("999.90"),
                Decimal("1029.89"),
                Decimal("49.99"),
                Decimal("79.99"),
                "USD",
                "John Doe",
                "NORTH",
                Decimal("38.50"),
                "MEDIUM",
                "ETL20240115",
                datetime(2024, 1, 15, 12, 0, 0),
                "ETL_USER",
            )
        ]
        
        df = spark.createDataFrame(data, schema)
        
        assert df.count() == 1
        row = df.first()
        assert row.analytics_id == "ANL001"
        assert row.category == "MEDIUM"
        assert row.profit_margin == Decimal("38.50")


class TestETLLogSchema:
    """Test ETL log schema definition."""
    
    def test_schema_structure(self):
        """Test that ETL log schema has correct structure."""
        schema = ETLSchemas.etl_log_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 12
    
    def test_field_types(self):
        """Test ETL log field data types."""
        schema = ETLSchemas.etl_log_schema()
        field_map = {field.name: field for field in schema.fields}
        
        assert isinstance(field_map["log_id"].dataType, StringType)
        assert isinstance(field_map["execution_date"].dataType, DateType)
        assert isinstance(field_map["execution_time"].dataType, StringType)
        assert isinstance(field_map["records_processed"].dataType, IntegerType)
        assert isinstance(field_map["created_at"].dataType, TimestampType)
    
    def test_create_log_dataframe(self, spark):
        """Test creating DataFrame with ETL log schema."""
        schema = ETLSchemas.etl_log_schema()
        
        data = [
            (
                "LOG001",
                "ETL20240115",
                date(2024, 1, 15),
                "10:30:00",
                "EXTRACT",
                "S",
                100,
                100,
                0,
                "Extraction completed successfully",
                datetime(2024, 1, 15, 10, 30, 0),
                "ETL_USER",
            )
        ]
        
        df = spark.createDataFrame(data, schema)
        
        assert df.count() == 1
        row = df.first()
        assert row.process_step == "EXTRACT"
        assert row.status == "S"
        assert row.records_processed == 100


class TestSchemaCompatibility:
    """Test schema compatibility and integration."""
    
    def test_raw_to_analytics_field_mapping(self):
        """Test that raw sales fields map to analytics fields."""
        raw_schema = ETLSchemas.raw_sales_schema()
        analytics_schema = ETLSchemas.analytics_schema()
        
        raw_fields = {field.name for field in raw_schema.fields}
        analytics_fields = {field.name for field in analytics_schema.fields}
        
        # Common fields between raw and analytics
        common_fields = {
            "trans_date", "customer_id", "product_id",
            "currency", "sales_rep", "region"
        }
        
        assert common_fields.issubset(raw_fields)
        assert common_fields.issubset(analytics_fields)
    
    def test_schema_field_count_consistency(self):
        """Test that all schemas have reasonable field counts."""
        raw_schema = ETLSchemas.raw_sales_schema()
        analytics_schema = ETLSchemas.analytics_schema()
        log_schema = ETLSchemas.etl_log_schema()
        
        # Expected field counts based on ABAP structures
        assert len(raw_schema.fields) == 12
        assert len(analytics_schema.fields) == 17
        assert len(log_schema.fields) == 12