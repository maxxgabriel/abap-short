"""
Unit tests for PySpark schema definitions.
Tests schema structure, field types, and nullability.
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
from src.schemas import ETLSchemas


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing"""
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_schemas")
        .getOrCreate()
    )


class TestRawSalesSchema:
    """Test suite for raw sales schema"""

    def test_schema_structure(self):
        """Test that raw sales schema has correct structure"""
        schema = ETLSchemas.raw_sales_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 12
        
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "trans_id",
            "trans_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "currency",
            "sales_rep",
            "region",
            "status",
            "created_at",
            "created_by",
        ]
        assert field_names == expected_fields

    def test_field_types(self):
        """Test that fields have correct data types"""
        schema = ETLSchemas.raw_sales_schema()
        field_types = {field.name: type(field.dataType) for field in schema.fields}
        
        assert field_types["trans_id"] == StringType
        assert field_types["trans_date"] == DateType
        assert field_types["customer_id"] == StringType
        assert field_types["product_id"] == StringType
        assert field_types["quantity"] == IntegerType
        assert field_types["unit_price"] == DecimalType
        assert field_types["currency"] == StringType
        assert field_types["status"] == StringType

    def test_nullable_constraints(self):
        """Test nullable constraints on fields"""
        schema = ETLSchemas.raw_sales_schema()
        nullable_map = {field.name: field.nullable for field in schema.fields}
        
        # Required fields
        assert nullable_map["trans_id"] is False
        assert nullable_map["trans_date"] is False
        assert nullable_map["customer_id"] is False
        assert nullable_map["product_id"] is False
        assert nullable_map["quantity"] is False
        assert nullable_map["unit_price"] is False
        assert nullable_map["currency"] is False
        assert nullable_map["status"] is False
        
        # Optional fields
        assert nullable_map["sales_rep"] is True
        assert nullable_map["region"] is True

    def test_decimal_precision(self):
        """Test decimal field precision and scale"""
        schema = ETLSchemas.raw_sales_schema()
        unit_price_field = next(f for f in schema.fields if f.name == "unit_price")
        
        assert unit_price_field.dataType.precision == 16
        assert unit_price_field.dataType.scale == 2

    def test_create_dataframe(self, spark):
        """Test creating DataFrame with schema"""
        schema = ETLSchemas.raw_sales_schema()
        
        data = [
            (
                "T000001",
                "2024-01-01",
                "CUST001",
                "PROD001",
                10,
                99.99,
                "USD",
                "John Doe",
                "NORTH",
                "N",
                "2024-01-01 10:00:00",
                "TESTUSER",
            )
        ]
        
        df = spark.createDataFrame(data, schema)
        assert df.count() == 1
        assert len(df.columns) == 12


class TestAnalyticsSchema:
    """Test suite for analytics schema"""

    def test_schema_structure(self):
        """Test that analytics schema has correct structure"""
        schema = ETLSchemas.analytics_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 17
        
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "analytics_id",
            "trans_date",
            "customer_id",
            "product_id",
            "total_quantity",
            "gross_amount",
            "net_amount",
            "discount_amount",
            "tax_amount",
            "currency",
            "sales_rep",
            "region",
            "profit_margin",
            "category",
            "etl_run_id",
            "loaded_at",
            "loaded_by",
        ]
        assert field_names == expected_fields

    def test_field_types(self):
        """Test that fields have correct data types"""
        schema = ETLSchemas.analytics_schema()
        field_types = {field.name: type(field.dataType) for field in schema.fields}
        
        assert field_types["analytics_id"] == StringType
        assert field_types["trans_date"] == DateType
        assert field_types["total_quantity"] == IntegerType
        assert field_types["gross_amount"] == DecimalType
        assert field_types["net_amount"] == DecimalType
        assert field_types["discount_amount"] == DecimalType
        assert field_types["tax_amount"] == DecimalType
        assert field_types["profit_margin"] == DecimalType
        assert field_types["category"] == StringType
        assert field_types["etl_run_id"] == StringType

    def test_decimal_fields_precision(self):
        """Test decimal fields have correct precision"""
        schema = ETLSchemas.analytics_schema()
        
        # Amount fields (16,2)
        amount_fields = [
            "gross_amount",
            "net_amount",
            "discount_amount",
            "tax_amount",
        ]
        for field_name in amount_fields:
            field = next(f for f in schema.fields if f.name == field_name)
            assert field.dataType.precision == 16
            assert field.dataType.scale == 2
        
        # Profit margin field (5,2)
        profit_field = next(f for f in schema.fields if f.name == "profit_margin")
        assert profit_field.dataType.precision == 5
        assert profit_field.dataType.scale == 2

    def test_nullable_constraints(self):
        """Test nullable constraints on fields"""
        schema = ETLSchemas.analytics_schema()
        nullable_map = {field.name: field.nullable for field in schema.fields}
        
        # Required fields
        assert nullable_map["analytics_id"] is False
        assert nullable_map["trans_date"] is False
        assert nullable_map["customer_id"] is False
        assert nullable_map["product_id"] is False
        assert nullable_map["total_quantity"] is False
        assert nullable_map["gross_amount"] is False
        assert nullable_map["category"] is False
        assert nullable_map["etl_run_id"] is False
        
        # Optional fields
        assert nullable_map["profit_margin"] is True
        assert nullable_map["sales_rep"] is True
        assert nullable_map["region"] is True


class TestETLLogSchema:
    """Test suite for ETL log schema"""

    def test_schema_structure(self):
        """Test that ETL log schema has correct structure"""
        schema = ETLSchemas.etl_log_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 12
        
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "log_id",
            "etl_run_id",
            "execution_date",
            "execution_time",
            "process_step",
            "status",
            "records_processed",
            "records_success",
            "records_error",
            "message",
            "created_at",
            "created_by",
        ]
        assert field_names == expected_fields

    def test_field_types(self):
        """Test that fields have correct data types"""
        schema = ETLSchemas.etl_log_schema()
        field_types = {field.name: type(field.dataType) for field in schema.fields}
        
        assert field_types["log_id"] == StringType
        assert field_types["etl_run_id"] == StringType
        assert field_types["execution_date"] == DateType
        assert field_types["execution_time"] == StringType
        assert field_types["process_step"] == StringType
        assert field_types["status"] == StringType
        assert field_types["records_processed"] == IntegerType
        assert field_types["records_success"] == IntegerType
        assert field_types["records_error"] == IntegerType
        assert field_types["message"] == StringType

    def test_create_dataframe(self, spark):
        """Test creating DataFrame with log schema"""
        schema = ETLSchemas.etl_log_schema()
        
        data = [
            (
                "LOG20240101100000",
                "ETL20240101100000",
                "2024-01-01",
                "10:00:00",
                "EXTRACT",
                "S",
                100,
                100,
                0,
                "Extraction completed successfully",
                "2024-01-01 10:00:00",
                "TESTUSER",
            )
        ]
        
        df = spark.createDataFrame(data, schema)
        assert df.count() == 1
        assert df.first()["status"] == "S"


class TestSchemaCompatibility:
    """Test schema compatibility and integration"""

    def test_schema_reusability(self, spark):
        """Test that schemas can be reused multiple times"""
        schema1 = ETLSchemas.raw_sales_schema()
        schema2 = ETLSchemas.raw_sales_schema()
        
        assert schema1 == schema2

    def test_empty_dataframe_creation(self, spark):
        """Test creating empty DataFrames with schemas"""
        raw_schema = ETLSchemas.raw_sales_schema()
        analytics_schema = ETLSchemas.analytics_schema()
        log_schema = ETLSchemas.etl_log_schema()
        
        df_raw = spark.createDataFrame([], raw_schema)
        df_analytics = spark.createDataFrame([], analytics_schema)
        df_log = spark.createDataFrame([], log_schema)
        
        assert df_raw.count() == 0
        assert df_analytics.count() == 0
        assert df_log.count() == 0
        
        assert len(df_raw.columns) == 12
        assert len(df_analytics.columns) == 17
        assert len(df_log.columns) == 12