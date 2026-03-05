"""
Pytest configuration and fixtures for integration tests.
"""
import pytest
import yaml
from datetime import datetime, timedelta
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from src.schemas import ETLSchemas
from src.constants import ETLConstants


@pytest.fixture(scope="session")
def config():
    """Load test configuration."""
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def spark(config):
    """Create Spark session for tests."""
    spark_config = config.get("spark", {})
    
    builder = SparkSession.builder.appName(spark_config.get("app_name", "ETL_Tests"))
    
    # Apply configuration
    for key, value in spark_config.get("config", {}).items():
        builder = builder.config(key, value)
    
    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    
    yield spark_session
    
    spark_session.stop()


@pytest.fixture
def sample_raw_data(spark, config):
    """Generate sample raw sales data for testing."""
    test_config = config.get("test_data", {})
    sample_size = test_config.get("sample_size", 10)
    regions = test_config.get("regions", ["NORTH", "SOUTH"])
    
    base_date = datetime.now().date()
    
    data = []
    for i in range(sample_size):
        data.append(Row(
            trans_id=f"T{i:06d}",
            trans_date=base_date - timedelta(days=i % 30),
            customer_id=f"CUST{(i % 5) + 1:03d}",
            product_id=f"PROD{(i % 3) + 1:03d}",
            quantity=5 + (i % 20),
            unit_price=Decimal("99.99") + Decimal(str(i * 10)),
            currency="USD",
            sales_rep=f"Rep_{i % 3}",
            region=regions[i % len(regions)],
            status=ETLConstants.STATUS.NEW,
            created_at=datetime.now(),
            created_by="TEST_USER"
        ))
    
    return spark.createDataFrame(data, schema=ETLSchemas.raw_sales_schema())


@pytest.fixture
def sample_analytics_data(spark):
    """Generate sample analytics data for testing."""
    base_date = datetime.now().date()
    
    data = [
        Row(
            analytics_id=f"ANL{i:010d}",
            trans_date=base_date,
            customer_id=f"CUST{i:03d}",
            product_id=f"PROD{i:03d}",
            total_quantity=10 + i,
            gross_amount=Decimal("1000.00") + Decimal(str(i * 100)),
            net_amount=Decimal("950.00") + Decimal(str(i * 100)),
            discount_amount=Decimal("50.00"),
            tax_amount=Decimal("76.00") + Decimal(str(i * 8)),
            currency="USD",
            sales_rep=f"Rep_{i}",
            region="NORTH",
            profit_margin=Decimal("25.50"),
            category=ETLConstants.CATEGORIES.MEDIUM,
            etl_run_id="ETL20240101120000",
            loaded_at=datetime.now(),
            loaded_by="TEST_USER"
        )
        for i in range(1, 6)
    ]
    
    return spark.createDataFrame(data, schema=ETLSchemas.analytics_schema())


@pytest.fixture
def temp_target_path(tmp_path):
    """Create temporary target path for output."""
    return str(tmp_path / "analytics_output")