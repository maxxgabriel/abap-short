import pytest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import yaml
from pathlib import Path


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing"""
    spark = (
        SparkSession.builder
        .appName("ETL_Integration_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="session")
def config():
    """Load test configuration"""
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def sample_dates():
    """Generate sample date range"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    return start_date, end_date


@pytest.fixture
def test_data_path(tmp_path):
    """Create temporary directory for test data"""
    return tmp_path / "test_data"