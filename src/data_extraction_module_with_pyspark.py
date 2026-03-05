===FILE: src/extract.py===
"""
Data Extraction Module with PySpark
Converts ABAP SELECT statements to PySpark read operations with date range filtering.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional, Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ExtractionError


class ETLExtractor:
    """
    Extracts raw sales data from source with date range filtering.
    Equivalent to ZCL_ETL_EXTRACTOR ABAP class.
    """
    
    # Schema mapping from ABAP internal table structure
    RAW_SALES_SCHEMA = StructType([
        StructField("trans_id", StringType(), nullable=False),
        StructField("trans_date", DateType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(16, 2), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("sales_rep", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("created_by", StringType(), nullable=True)
    ])
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor with Spark session and logger.
        
        Args:
            spark: Active SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary with connection parameters
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure Python logging for the extractor."""
        self.py_logger = logging.getLogger(__name__)
        self.py_logger.setLevel(logging.INFO)
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_type: str = "jdbc"
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data with date range filtering.
        Converts ABAP: SELECT * FROM zsales_raw WHERE trans_date BETWEEN ...
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            source_type: Source type (jdbc, parquet, csv, delta)
            
        Returns:
            Tuple of (DataFrame with extracted data, success boolean)
            
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Validate date range
            self._validate_date_range(from_date, to_date)
            
            # Extract based on source type
            if source_type == "jdbc":
                df = self._extract_from_jdbc(from_date, to_date)
            elif source_type == "parquet":
                df = self._extract_from_parquet(from_date, to_date)
            elif source_type == "csv":
                df = self._extract_from_csv(from_date, to_date)
            elif source_type == "delta":
                df = self._extract_from_delta(from_date, to_date)
            else:
                raise ExtractionError(
                    f"Unsupported source type: {source_type}",
                    step="EXTRACT"
                )
            
            # Apply date range filter and status filter
            df = self._apply_filters(df, from_date, to_date)
            
            # Validate schema
            self._validate_schema(df)
            
            # Cache for reuse
            df.cache()
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df, True
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            self.py_logger.error(f"Extraction error: {str(e)}", exc_info=True)
            raise ExtractionError(str(e), step="EXTRACT") from e
    
    def _validate_date_range(self, from_date: str, to_date: str):
        """Validate date range parameters."""
        try:
            start = datetime.strptime(from_date, "%Y-%m-%d")
            end = datetime.strptime(to_date, "%Y-%m-%d")
            
            if start > end:
                raise ValueError("from_date cannot be later than to_date")
            
            if end > datetime.now():
                raise ValueError("to_date cannot be in the future")
                
        except ValueError as e:
            raise ExtractionError(f"Invalid date range: {str(e)}", step="EXTRACT")
    
    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract from JDBC source (SAP HANA, Oracle, etc.).
        Equivalent to ABAP SELECT with WHERE clause.
        """
        jdbc_config = self.config.get("source", {}).get("jdbc", {})
        
        # Build push-down query for optimal performance
        query = f"""
            (SELECT * FROM {jdbc_config.get('table', 'zsales_raw')}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') AS sales_data
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver")) \
            .option("fetchsize", jdbc_config.get("fetchsize", 1000)) \
            .option("numPartitions", jdbc_config.get("num_partitions", 4)) \
            .load()
        
        return df
    
    def _extract_from_parquet(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from Parquet files with partition pruning."""
        parquet_path = self.config.get("source", {}).get("parquet_path")
        
        df = self.spark.read \
            .schema(self.RAW_SALES_SCHEMA) \
            .parquet(parquet_path)
        
        return df
    
    def _extract_from_csv(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from CSV files."""
        csv_config = self.config.get("source", {}).get("csv", {})
        csv_path = csv_config.get("path")
        
        df = self.spark.read \
            .schema(self.RAW_SALES_SCHEMA) \
            .option("header", csv_config.get("header", "true")) \
            .option("delimiter", csv_config.get("delimiter", ",")) \
            .option("mode", "FAILFAST") \
            .csv(csv_path)
        
        return df
    
    def _extract_from_delta(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from Delta Lake with time travel support."""
        delta_path = self.config.get("source", {}).get("delta_path")
        
        df = self.spark.read \
            .format("delta") \
            .load(delta_path)
        
        return df
    
    def _apply_filters(self, df: DataFrame, from_date: str, to_date: str) -> DataFrame:
        """
        Apply date range and status filters.
        Equivalent to ABAP WHERE clause.
        """
        filtered_df = df.filter(
            (F.col("trans_date") >= F.lit(from_date)) &
            (F.col("trans_date") <= F.lit(to_date)) &
            (F.col("status") == F.lit("N"))
        )
        
        return filtered_df
    
    def _validate_schema(self, df: DataFrame):
        """Validate DataFrame schema matches expected structure."""
        df_fields = {field.name for field in df.schema.fields}
        expected_fields = {field.name for field in self.RAW_SALES_SCHEMA.fields}
        
        missing_fields = expected_fields - df_fields
        if missing_fields:
            raise ExtractionError(
                f"Missing required fields: {missing_fields}",
                step="EXTRACT"
            )
    
    def extract_incremental(
        self, 
        watermark_column: str = "created_at",
        last_watermark: Optional[str] = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract data incrementally based on watermark column.
        
        Args:
            watermark_column: Column to use for incremental extraction
            last_watermark: Last processed watermark value
            
        Returns:
            Tuple of (DataFrame, success boolean)
        """
        try:
            source_config = self.config.get("source", {})
            
            if source_config.get("type") == "jdbc":
                jdbc_config = source_config.get("jdbc", {})
                
                if last_watermark:
                    query = f"""
                        (SELECT * FROM {jdbc_config.get('table')}
                         WHERE {watermark_column} > '{last_watermark}'
                         AND status = 'N') AS incremental_data
                    """
                else:
                    query = f"""
                        (SELECT * FROM {jdbc_config.get('table')}
                         WHERE status = 'N') AS incremental_data
                    """
                
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_config.get("url")) \
                    .option("dbtable", query) \
                    .option("user", jdbc_config.get("user")) \
                    .option("password", jdbc_config.get("password")) \
                    .option("driver", jdbc_config.get("driver")) \
                    .load()
                
                record_count = df.count()
                
                self.logger.log_message(
                    step="EXTRACT_INCREMENTAL",
                    status="S",
                    records_processed=record_count,
                    message=f"Extracted {record_count} incremental records"
                )
                
                return df, True
            else:
                raise ExtractionError(
                    "Incremental extraction only supported for JDBC sources",
                    step="EXTRACT_INCREMENTAL"
                )
                
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT_INCREMENTAL",
                status="E",
                message=f"Incremental extraction failed: {str(e)}"
            )
            raise ExtractionError(str(e), step="EXTRACT_INCREMENTAL") from e


===FILE: src/logger.py===
"""
ETL Logger Module
Provides logging functionality equivalent to ZCL_ETL_LOGGER ABAP class.
"""

from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """
    Logger for ETL process with structured logging.
    Equivalent to ZCL_ETL_LOGGER ABAP class.
    """
    
    def __init__(self, etl_run_id: str):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Setup Python logger
        self.logger = logging.getLogger(f"ETL_{etl_run_id}")
        self.logger.setLevel(logging.INFO)
        
        # Create console handler
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """
        Log a message with ETL context.
        
        Args:
            step: ETL process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of failed records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Error: {records_error}"
        )
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID based on timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp[:14]}"
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to Python logging level."""
        status_map = {
            "S": logging.INFO,
            "E": logging.ERROR,
            "W": logging.WARNING,
            "I": logging.INFO
        }
        return status_map.get(status, logging.INFO)
    
    def get_etl_run_id(self) -> str:
        """Return the ETL run ID."""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Return all log entries for this ETL run."""
        return self.log_entries
    
    def get_statistics(self) -> dict:
        """Calculate statistics from log entries."""
        total_processed = sum(e["records_processed"] for e in self.log_entries)
        total_success = sum(e["records_success"] for e in self.log_entries)
        total_error = sum(e["records_error"] for e in self.log_entries)
        
        error_count = sum(1 for e in self.log_entries if e["status"] == "E")
        warning_count = sum(1 for e in self.log_entries if e["status"] == "W")
        
        return {
            "total_records_processed": total_processed,
            "total_records_success": total_success,
            "total_records_error": total_error,
            "error_count": error_count,
            "warning_count": warning_count
        }


===FILE: src/exceptions.py===
"""
Custom exceptions for ETL process.
Equivalent to ZCX_ETL_ERROR ABAP exception class.
"""


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(self, message: str, step: str = "", record_id: str = ""):
        """
        Initialize ETL exception.
        
        Args:
            message: Error message
            step: ETL step where error occurred
            record_id: Record ID that caused the error (if applicable)
        """
        self.message = message
        self.step = step
        self.record_id = record_id
        super().__init__(self.message)
    
    def __str__(self):
        error_parts = [f"ETL Error: {self.message}"]
        if self.step:
            error_parts.append(f"Step: {self.step}")
        if self.record_id:
            error_parts.append(f"Record ID: {self.record_id}")
        return " | ".join(error_parts)


class ExtractionError(ETLError):
    """Exception raised during data extraction."""
    pass


class TransformationError(ETLError):
    """Exception raised during data transformation."""
    pass


class LoadError(ETLError):
    """Exception raised during data loading."""
    pass


class ValidationError(ETLError):
    """Exception raised during data validation."""
    pass


class ConfigurationError(ETLError):
    """Exception raised for configuration issues."""
    pass


===FILE: config.yaml===
# ETL Configuration
# Equivalent to configuration from ZCL_ETL_CONSTANTS and runtime parameters

etl:
  # ETL Process Configuration
  process:
    batch_size: 1000
    commit_interval: 500
    retry_attempts: 3
    timeout_seconds: 3600
    parallel_jobs: 4
  
  # Status codes
  status:
    new: "N"
    processed: "P"
    error: "E"
    warning: "W"
    success: "S"
    info: "I"
  
  # Process steps
  steps:
    init: "INIT"
    extract: "EXTRACT"
    transform: "TRANSFORM"
    load: "LOAD"
    validate: "VALIDATE"
    complete: "COMPLETE"
    error: "ERROR"

# Business Rules (from ZCL_ETL_CONSTANTS)
business_rules:
  # Discount thresholds
  discount:
    qty_tier1: 10
    qty_tier2: 15
    rate_tier1: 0.05
    rate_tier2: 0.10
  
  # Tax rate
  tax_rate: 0.08
  
  # Cost ratio for profit margin calculation
  cost_ratio: 0.60
  
  # Category thresholds
  category:
    high_threshold: 2000.00
    medium_threshold: 500.00
    values:
      high: "HIGH"
      medium: "MEDIUM"
      low: "LOW"

# Data Source Configuration
source:
  # Source type: jdbc, parquet, csv, delta
  type: "jdbc"
  
  # JDBC configuration (for SAP HANA, Oracle, etc.)
  jdbc:
    url: "jdbc:sap://localhost:30015"
    driver: "com.sap.db.jdbc.Driver"
    table: "zsales_raw"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    fetchsize: 1000
    num_partitions: 4
    connection_properties:
      socketTimeout: 300000
      queryTimeout: 300
  
  # Parquet configuration
  parquet_path: "/data/raw/sales"
  
  # CSV configuration
  csv:
    path: "/data/raw/sales/*.csv"
    header: true
    delimiter: ","
    encoding: "UTF-8"
  
  # Delta Lake configuration
  delta_path: "/data/raw/sales_delta"

# Data Target Configuration
target:
  # Target type: jdbc, parquet, delta
  type: "jdbc"
  
  # JDBC configuration
  jdbc:
    url: "jdbc:sap://localhost:30015"
    driver: "com.sap.db.jdbc.Driver"
    table: "zsales_analytics"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    write_mode: "append"  # append, overwrite, error, ignore
    batch_size: 1000
  
  # Parquet configuration
  parquet_path: "/data/analytics/sales"
  parquet_mode: "append"
  parquet_partitions: ["trans_date"]
  
  # Delta configuration
  delta_path: "/data/analytics/sales_delta"
  delta_mode: "append"
  delta_optimize: true
  delta_vacuum_hours: 168  # 7 days

# Logging Configuration
logging:
  # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
  level: "INFO"
  
  # Log format
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  
  # Log output
  console: true
  file: true
  file_path: "/var/log/etl/sales_etl.log"
  
  # Database logging (equivalent to ZETL_LOG table)
  database:
    enabled: true
    table: "zetl_log"
    jdbc_url: "jdbc:sap://localhost:30015"
    driver: "com.sap.db.jdbc.Driver"

# Spark Configuration
spark:
  app_name: "SalesETL"
  master: "local[*]"  # Override for cluster: yarn, spark://host:port
  
  # Spark SQL configuration
  config:
    spark.sql.shuffle.partitions: 200
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
    spark.serializer: "org.apache.spark.serializer.KryoSerializer"
    spark.sql.sources.partitionOverwriteMode: "dynamic"
    
  # Memory configuration
  driver_memory: "4g"
  executor_memory: "4g"
  executor_cores: 2
  
  # Optimization
  dynamic_allocation: true
  shuffle_partitions: 200

# Date Range Configuration
date_range:
  # Default date range (days from today)
  default_from_days: -7
  default_to_days: 0
  
  # Date format
  format: "%Y-%m-%d"

# Monitoring and Alerting
monitoring:
  enabled: true
  metrics_path: "/metrics/etl"
  alert_on_failure: true
  alert_email: "etl-alerts@company.com"


===FILE: tests/test_extract.py===
"""
Unit tests for ETL Extractor module.
Tests extraction logic, date filtering, and error handling.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from datetime import datetime, timedelta
import yaml

from src.extract import ETLExtractor
from src.logger import ETLLogger
from src.exceptions import ExtractionError


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .appName("TestETLExtractor") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def test_config():
    """Load test configuration."""
    config = {
        "source": {
            "type": "csv",
            "csv": {
                "path": "tests/data/sample_sales.csv",
                "header": "true",
                "delimiter": ","
            }
        },
        "etl": {
            "process": {
                "batch_size": 100
            }
        }
    }
    return config


@pytest.fixture
def etl_logger():
    """Create ETL logger for tests."""
    run_id = f"TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    return ETLLogger(run_id)


@pytest.fixture
def sample_data(spark):
    """Create sample sales data for testing."""
    schema = StructType([
        StructField("trans_id", StringType(), False),
        StructField("trans_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), False),
    ])
    
    data = [
        ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
        ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
        ("T000004", (datetime.now() - timedelta(days=5)).date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
        ("T000005", (datetime.now() - timedelta(days=10)).date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "P"),
    ]
    
    return spark.createDataFrame(data, schema)


class TestETLExtractor:
    """Test cases for ETLExtractor class."""
    
    def test_extractor_initialization(self, spark, etl_logger, test_config):
        """Test extractor initializes correctly."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        assert extractor.spark is not None
        assert extractor.logger is not None
        assert extractor.config == test_config
    
    def test_date_range_validation_valid(self, spark, etl_logger, test_config):
        """Test valid date range validation."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Should not raise exception
        extractor._validate_date_range(from_date, to_date)
    
    def test_date_range_validation_invalid_order(self, spark, etl_logger, test_config):
        """Test date range validation with invalid order."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        from_date = datetime.now().strftime("%Y-%m-%d")
        to_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        with pytest.raises(ExtractionError) as exc_info:
            extractor._validate_date_range(from_date, to_date)
        
        assert "cannot be later than" in str(exc_info.value)
    
    def test_date_range_validation_future_date(self, spark, etl_logger, test_config):
        """Test date range validation with future date."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        from_date = datetime.now().strftime("%Y-%m-%d")
        to_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        with pytest.raises(ExtractionError) as exc_info:
            extractor._validate_date_range(from_date, to_date)
        
        assert "cannot be in the future" in str(exc_info.value)
    
    def test_apply_filters(self, spark, etl_logger, test_config, sample_data):
        """Test date range and status filtering."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        filtered_df = extractor._apply_filters(sample_data, from_date, to_date)
        
        # Should have 4 records (status='N' and within date range)
        assert filtered_df.count() == 4
        
        # All records should have status='N'
        statuses = filtered_df.select("status").distinct().collect()
        assert len(statuses) == 1
        assert statuses[0]["status"] == "N"
    
    def test_schema_validation_valid(self, spark, etl_logger, test_config, sample_data):
        """Test schema validation with valid schema."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        # Should not raise exception
        extractor._validate_schema(sample_data)
    
    def test_schema_validation_missing_fields(self, spark, etl_logger, test_config):
        """Test schema validation with missing fields."""
        extractor = ETLExtractor(spark, etl_logger, test_config)
        
        # Create DataFrame with missing fields
        incomplete_df = spark.createDataFrame(
            [("T001", "CUST001")],
            ["trans_id", "customer_id"]
        )
        
        with pytest.raises(ExtractionError) as exc_info:
            extractor._validate_schema(incomplete_df)
        
        assert "Missing required fields" in str(exc_info.value)
    
    def test_extract_from_csv_mock(self, spark, etl_logger, test_config, tmp_path):
        """Test extraction from CSV source."""
        # Create temporary CSV file
        csv_path = tmp_path / "test_sales.csv"
        csv_content = """trans_id,trans_date,customer_id,product_id,quantity,unit_price,currency,sales_rep,region,status
T000001,2024-01-15,CUST001,PROD001,10,99.99,USD,John Doe,NORTH,N
T000002,2024-01-15,CUST002,PROD002,5,149.99,USD,Jane Smith,SOUTH,N"""
        
        csv_path.write_text(csv_content)
        
        # Update config with test path
        test_config["source"]["csv"]["path"] = str(csv_path)
        
        extractor = ETLExtractor(spark, etl_logger, test_config)
        df = extractor._extract_from_csv("2024-01-01", "2024-12-31")
        
        assert df is not None
        assert df.count() == 2
    
    def test_extractor_logging(self, spark, etl_logger, test_config, sample_data, tmp_path):
        """Test that extractor logs messages correctly."""
        # Create temporary CSV
        csv_path = tmp_path / "test_sales.csv"
        csv_content = """trans_id,trans_date,customer_id,product_id,quantity,unit_price,currency,sales_rep,region,status
T000001,2