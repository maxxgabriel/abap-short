"""
Unit tests for ETL Loader module.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import date
import yaml

from src.load import ETLLoader
from src.logger import ETLLogger
from src.exceptions import ETLLoadError


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("test_etl_loader") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def config():
    """Load test configuration."""
    return {
        'load': {
            'target_path': '/tmp/test_analytics',
            'target_format': 'parquet',
            'write_mode': 'overwrite',
            'partition_columns': ['region'],
            'write_options': {'compression': 'snappy'},
            'expected_columns': [
                'analytics_id', 'trans_date', 'customer_id', 'product_id',
                'total_quantity', 'gross_amount', 'net_amount', 'discount_amount',
                'tax_amount', 'currency', 'sales_rep', 'region', 'profit_margin',
                'category', 'etl_run_id'
            ]
        },
        'extract': {
            'source_path': '/tmp/test_raw'
        },
        'logging': {
            'log_path': '/tmp/test_logs',
            'persist_logs': False
        }
    }


@pytest.fixture
def sample_analytics_schema():
    """Define analytics data schema."""
    return StructType([
        StructField("analytics_id", StringType(), False),
        StructField("trans_id", StringType(), False),
        StructField("trans_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("total_quantity", IntegerType(), False),
        StructField("gross_amount", DecimalType(16, 2), False),
        StructField("net_amount", DecimalType(16, 2), False),
        StructField("discount_amount", DecimalType(16, 2), False),
        StructField("tax_amount", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("profit_margin", DecimalType(5, 2), True),
        StructField("category", StringType(), False),
        StructField("etl_run_id", StringType(), False)
    ])


@pytest.fixture
def sample_analytics_data(spark, sample_analytics_schema):
    """Create sample analytics data."""
    data = [
        (
            "ANL001", "T001", date(2024, 1, 15), "CUST001", "PROD001",
            10, 999.90, 1079.89, 0.00, 79.99, "USD",
            "John Doe", "NORTH", 35.50, "MEDIUM", "ETL20240115120000"
        ),
        (
            "ANL002", "T002", date(2024, 1, 15), "CUST002", "PROD002",
            5, 749.95, 809.95, 0.00, 59.99, "USD",
            "Jane Smith", "SOUTH", 38.20, "MEDIUM", "ETL20240115120000"
        ),
        (
            "ANL003", "T003", date(2024, 1, 15), "CUST003", "PROD001",
            20, 1999.80, 1943.81, 199.98, 143.91, "USD",
            "John Doe", "EAST", 36.75, "MEDIUM", "ETL20240115120000"
        ),
        (
            "ANL004", "T004", date(2024, 1, 15), "CUST001", "PROD003",
            3, 899.97, 971.97, 0.00, 71.99, "USD",
            "Bob Wilson", "WEST", 34.10, "MEDIUM", "ETL20240115120000"
        ),
        (
            "ANL005", "T005", date(2024, 1, 15), "CUST004", "PROD002",
            15, 2249.85, 2187.36, 224.98, 161.98, "USD",
            "Jane Smith", "SOUTH", 37.90, "HIGH", "ETL20240115120000"
        )
    ]
    
    return spark.createDataFrame(data, sample_analytics_schema)


def test_loader_initialization(spark, config):
    """Test ETL loader initialization."""
    logger = ETLLogger(spark, "TEST_RUN_001", config)
    loader = ETLLoader(spark, logger, config)
    
    assert loader.spark is not None
    assert loader.logger is not None
    assert loader.config == config


def test_validate_records_all_valid(spark, config, sample_analytics_data):
    """Test validation with all valid records."""
    logger = ETLLogger(spark, "TEST_RUN_002", config)
    loader = ETLLoader(spark, logger, config)
    
    validated_df, stats = loader._validate_records(sample_analytics_data)
    
    assert stats['total_records'] == 5
    assert stats['valid_records'] == 5
    assert stats['invalid_records'] == 0
    assert validated_df.count() == 5


def test_validate_records_with_invalid(spark, config, sample_analytics_schema):
    """Test validation with invalid records."""
    logger = ETLLogger(spark, "TEST_RUN_003", config)
    loader = ETLLoader(spark, logger, config)
    
    # Create data with invalid records
    data = [
        (
            "ANL001", "T001", date(2024, 1, 15), "CUST001", "PROD001",
            10, 999.90, 1079.89, 0.00, 79.99, "USD",
            "John Doe", "NORTH", 35.50, "MEDIUM", "ETL20240115120000"
        ),
        (
            None, "T002", date(2024, 1, 15), "CUST002", "PROD002",  # Invalid: null analytics_id
            5, 749.95, 809.95, 0.00, 59.99, "USD",
            "Jane Smith", "SOUTH", 38.20, "MEDIUM", "ETL20240115120000"
        ),
        (
            "ANL003", "T003", date(2024, 1, 15), "CUST003", None,  # Invalid: null product_id
            20, 1999.80, 1943.81, 199.98, 143.91, "USD",
            "John Doe", "EAST", 36.75, "MEDIUM", "ETL20240115120000"
        )
    ]
    
    df = spark.createDataFrame(data, sample_analytics_schema)
    validated_df, stats = loader._validate_records(df)
    
    assert stats['total_records'] == 3
    assert stats['valid_records'] == 1
    assert stats['invalid_records'] == 2


def test_add_metadata(spark, config, sample_analytics_data):
    """Test metadata addition to DataFrame."""
    logger = ETLLogger(spark, "TEST_RUN_004", config)
    loader = ETLLoader(spark, logger, config)
    
    etl_run_id = "TEST_ETL_RUN_001"
    enriched_df = loader._add_metadata(sample_analytics_data, etl_run_id)
    
    assert 'loaded_at' in enriched_df.columns
    assert 'loaded_by' in enriched_df.columns
    assert 'etl_run_id' in enriched_df.columns
    
    # Verify etl_run_id value
    row = enriched_df.first()
    assert row['etl_run_id'] == etl_run_id


def test_validate_target_schema_valid(spark, config, sample_analytics_data):
    """Test schema validation with valid schema."""
    logger = ETLLogger(spark, "TEST_RUN_005", config)
    loader = ETLLoader(spark, logger, config)
    
    is_valid = loader.validate_target_schema(sample_analytics_data)
    assert is_valid is True


def test_validate_target_schema_missing_columns(spark, config, sample_analytics_data):
    """Test schema validation with missing columns."""
    logger = ETLLogger(spark, "TEST_RUN_006", config)
    loader = ETLLoader(spark, logger, config)
    
    # Drop a required column
    incomplete_df = sample_analytics_data.drop('customer_id')
    
    is_valid = loader.validate_target_schema(incomplete_df)
    assert is_valid is False


def test_load_data_success(spark, config, sample_analytics_data):
    """Test successful data load."""
    logger = ETLLogger(spark, "TEST_RUN_007", config)
    loader = ETLLoader(spark, logger, config)
    
    success, stats = loader.load_data(sample_analytics_data, "ETL_RUN_007")
    
    assert success is True
    assert stats['total_records'] == 5
    assert stats['success_records'] == 5
    assert stats['error_records'] == 0


def test_compile_statistics(spark, config, sample_analytics_data):
    """Test statistics compilation."""
    logger = ETLLogger(spark, "TEST_RUN_008", config)
    loader = ETLLoader(spark, logger, config)
    
    validation_stats = {
        'total_records': 5,
        'valid_records': 4,
        'invalid_records': 1
    }
    
    stats = loader._compile_statistics(sample_analytics_data, validation_stats)
    
    assert stats['total_records'] == 5
    assert stats['success_records'] == 4
    assert stats['error_records'] == 1


def test_write_to_target(spark, config, sample_analytics_data):
    """Test writing data to target location."""
    logger = ETLLogger(spark, "TEST_RUN_009", config)
    loader = ETLLoader(spark, logger, config)
    
    # Add metadata before writing
    enriched_df = loader._add_metadata(sample_analytics_data, "ETL_RUN_009")
    
    success = loader._write_to_target(enriched_df)
    assert success is True
    
    # Verify data was written
    target_path = config['load']['target_path']
    written_df = spark.read.format('parquet').load(target_path)
    assert written_df.count() == 5


def test_load_with_invalid_records(spark, config, sample_analytics_schema):
    """Test load handling with invalid records."""
    logger = ETLLogger(spark, "TEST_RUN_010", config)
    loader = ETLLoader(spark, logger, config)
    
    # Create mixed valid/invalid data
    data = [
        (
            "ANL001", "T001", date(2024, 1, 15), "CUST001", "PROD001",
            10, 999.90, 1079.89, 0.00, 79.99, "USD",
            "John Doe", "NORTH", 35.50, "MEDIUM", "ETL20240115120000"
        ),
        (
            "ANL002", "T002", date(2024, 1, 15), None, "PROD002",  # Invalid
            5, 749.95, 809.95, 0.00, 59.99, "USD",
            "Jane Smith", "SOUTH", 38.20, "MEDIUM", "ETL20240115120000"
        )
    ]
    
    df = spark.createDataFrame(data, sample_analytics_schema)
    success, stats = loader.load_data(df, "ETL_RUN_010")
    
    assert success is True
    assert stats['success_records'] == 1
    assert stats['error_records'] == 1