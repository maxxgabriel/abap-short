"""
Pytest configuration and shared fixtures for ETL logger tests
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing.
    Shared across all tests in the session.
    """
    spark = (
        SparkSession.builder
        .appName("ETLLoggerTests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_config():
    """
    Provide sample configuration for testing
    """
    return {
        'log_table': 'test_etl_logs',
        'console_level': 'INFO',
        'enable_database_logging': True,
        'flush_interval': 100
    }