"""
Pytest configuration and shared fixtures
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing
    """
    spark = (
        SparkSession.builder
        .appName("ETL_Logger_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_etl_run_id():
    """
    Sample ETL run ID for testing
    """
    return "ETL20240101120000"