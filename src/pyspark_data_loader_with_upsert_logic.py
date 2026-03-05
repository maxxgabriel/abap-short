===FILE: src/load.py===
"""
PySpark Data Loader Module
Implements upsert logic for loading transformed analytics data into target tables.
Migrated from ABAP ZCL_ETL_LOADER class.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Dict, Tuple
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class DataLoader:
    """
    Loads transformed analytics data into target table with upsert capability.
    Supports both INSERT (new records) and UPDATE (existing records) operations.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict):
        """
        Initialize DataLoader with Spark session and configuration.
        
        Args:
            spark: Active SparkSession instance
            logger: ETL logger instance for tracking operations
            config: Configuration dictionary with load settings
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.target_table = config.get('target_table', 'sales_analytics')
        self.staging_table = f"{self.target_table}_staging"
        self.batch_size = config.get('batch_size', 1000)
        self.enable_validation = config.get('enable_validation', True)
        
    def get_target_schema(self) -> StructType:
        """
        Define the target analytics table schema.
        
        Returns:
            StructType schema matching analytics data structure
        """
        return StructType([
            StructField("analytics_id", StringType(), False),
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), False),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_by", StringType(), True)
        ])
    
    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading using DataFrame transformations.
        Implements validation rules from ABAP validate_record method.
        
        Args:
            df: DataFrame with analytics records to validate
            
        Returns:
            DataFrame with validation status column
        """
        if not self.enable_validation:
            return df.withColumn("is_valid", F.lit(True))
        
        # Apply validation rules using map transformation pattern
        validation_conditions = [
            F.col("analytics_id").isNotNull(),
            F.col("customer_id").isNotNull(),
            F.col("product_id").isNotNull(),
            F.col("gross_amount") > 0,
            F.col("currency").isNotNull(),
            F.col("category").isin("HIGH", "MEDIUM", "LOW")
        ]
        
        # Combine all validation conditions
        df_validated = df.withColumn(
            "is_valid",
            F.when(
                F.expr(" AND ".join([str(cond) for cond in validation_conditions])),
                True
            ).otherwise(False)
        )
        
        # Add validation message for failed records
        df_validated = df_validated.withColumn(
            "validation_message",
            F.when(
                ~F.col("is_valid"),
                F.concat_ws("; ",
                    F.when(F.col("analytics_id").isNull(), F.lit("Missing analytics_id")),
                    F.when(F.col("customer_id").isNull(), F.lit("Missing customer_id")),
                    F.when(F.col("product_id").isNull(), F.lit("Missing product_id")),
                    F.when(F.col("gross_amount") <= 0, F.lit("Invalid gross_amount")),
                    F.when(F.col("currency").isNull(), F.lit("Missing currency")),
                    F.when(~F.col("category").isin("HIGH", "MEDIUM", "LOW"), F.lit("Invalid category"))
                )
            ).otherwise(F.lit("Valid"))
        )
        
        return df_validated
    
    def prepare_upsert_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Separate data into INSERT and UPDATE operations based on existing records.
        
        Args:
            df: Input DataFrame with analytics data
            
        Returns:
            Tuple of (insert_df, update_df) DataFrames
        """
        # Add metadata columns for new records
        df_prepared = df.withColumn("loaded_at", F.current_timestamp()) \
                       .withColumn("loaded_by", F.lit(self.config.get('user', 'spark_etl'))) \
                       .withColumn("updated_at", F.lit(None).cast(TimestampType())) \
                       .withColumn("updated_by", F.lit(None).cast(StringType()))
        
        # Check if target table exists
        try:
            existing_df = self.spark.read.table(self.target_table)
            existing_keys = existing_df.select("analytics_id")
            
            # Split into inserts (new) and updates (existing)
            insert_df = df_prepared.join(
                existing_keys,
                on="analytics_id",
                how="left_anti"
            )
            
            update_df = df_prepared.join(
                existing_keys,
                on="analytics_id",
                how="inner"
            ).withColumn("updated_at", F.current_timestamp()) \
             .withColumn("updated_by", F.lit(self.config.get('user', 'spark_etl')))
            
            return insert_df, update_df
            
        except Exception:
            # If table doesn't exist, all records are inserts
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Target table {self.target_table} not found. All records will be inserted."
            )
            return df_prepared, self.spark.createDataFrame([], self.get_target_schema())
    
    def perform_upsert(self, insert_df: DataFrame, update_df: DataFrame) -> Dict[str, int]:
        """
        Execute upsert operation: INSERT new records and UPDATE existing ones.
        
        Args:
            insert_df: DataFrame with records to insert
            update_df: DataFrame with records to update
            
        Returns:
            Dictionary with operation statistics
        """
        stats = {
            'inserted': 0,
            'updated': 0,
            'failed': 0
        }
        
        try:
            # Write inserts to staging table first
            if insert_df.count() > 0:
                insert_df.write \
                    .format(self.config.get('format', 'parquet')) \
                    .mode('append') \
                    .option("mergeSchema", "true") \
                    .saveAsTable(self.target_table)
                
                stats['inserted'] = insert_df.count()
                
                self.logger.log_message(
                    step="LOAD",
                    status="S",
                    records_processed=stats['inserted'],
                    records_success=stats['inserted'],
                    message=f"Inserted {stats['inserted']} new records"
                )
            
            # Handle updates using merge logic
            if update_df.count() > 0:
                # Create temporary view for merge operation
                update_df.createOrReplaceTempView("updates_temp")
                
                # Execute merge using SQL
                merge_sql = f"""
                MERGE INTO {self.target_table} target
                USING updates_temp source
                ON target.analytics_id = source.analytics_id
                WHEN MATCHED THEN UPDATE SET
                    target.trans_date = source.trans_date,
                    target.customer_id = source.customer_id,
                    target.product_id = source.product_id,
                    target.total_quantity = source.total_quantity,
                    target.gross_amount = source.gross_amount,
                    target.net_amount = source.net_amount,
                    target.discount_amount = source.discount_amount,
                    target.tax_amount = source.tax_amount,
                    target.currency = source.currency,
                    target.sales_rep = source.sales_rep,
                    target.region = source.region,
                    target.profit_margin = source.profit_margin,
                    target.category = source.category,
                    target.etl_run_id = source.etl_run_id,
                    target.updated_at = source.updated_at,
                    target.updated_by = source.updated_by
                """
                
                self.spark.sql(merge_sql)
                stats['updated'] = update_df.count()
                
                self.logger.log_message(
                    step="LOAD",
                    status="S",
                    records_processed=stats['updated'],
                    records_success=stats['updated'],
                    message=f"Updated {stats['updated']} existing records"
                )
            
            return stats
            
        except Exception as e:
            raise ETLLoadError(f"Upsert operation failed: {str(e)}")
    
    def update_source_status(self, processed_ids: list) -> bool:
        """
        Update status of processed records in source table.
        Implements status update from ABAP load_data method.
        
        Args:
            processed_ids: List of transaction IDs that were processed
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            source_table = self.config.get('source_table', 'sales_raw')
            
            # Create DataFrame with processed IDs
            ids_df = self.spark.createDataFrame(
                [(id_val,) for id_val in processed_ids],
                ["trans_id"]
            )
            
            # Update status using SQL
            ids_df.createOrReplaceTempView("processed_ids")
            
            update_sql = f"""
            UPDATE {source_table}
            SET status = 'P', processed_at = current_timestamp()
            WHERE trans_id IN (SELECT trans_id FROM processed_ids)
            """
            
            self.spark.sql(update_sql)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                message=f"Updated status for {len(processed_ids)} source records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Failed to update source status: {str(e)}"
            )
            return False
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Main load method implementing complete upsert logic.
        Migrated from ABAP ZCL_ETL_LOADER->load_data method.
        
        Args:
            analytics_df: DataFrame with transformed analytics data
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load with upsert logic"
            )
            
            total_count = analytics_df.count()
            
            # Step 1: Validate records
            validated_df = self.validate_record(analytics_df)
            
            # Separate valid and invalid records
            valid_df = validated_df.filter(F.col("is_valid") == True) \
                                   .drop("is_valid", "validation_message")
            invalid_df = validated_df.filter(F.col("is_valid") == False)
            
            invalid_count = invalid_df.count()
            if invalid_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    records_error=invalid_count,
                    message=f"Skipped {invalid_count} invalid records"
                )
                
                # Log sample invalid records for debugging
                invalid_sample = invalid_df.select("analytics_id", "validation_message") \
                                          .limit(10) \
                                          .collect()
                for row in invalid_sample:
                    self.logger.log_message(
                        step="LOAD",
                        status="W",
                        message=f"Invalid record {row.analytics_id}: {row.validation_message}"
                    )
            
            # Step 2: Prepare upsert data
            insert_df, update_df = self.prepare_upsert_data(valid_df)
            
            # Step 3: Perform upsert
            stats = self.perform_upsert(insert_df, update_df)
            
            # Step 4: Update source table status
            processed_ids = valid_df.select("analytics_id").rdd.flatMap(lambda x: x).collect()
            self.update_source_status(processed_ids)
            
            # Log final statistics
            success_count = stats['inserted'] + stats['updated']
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=invalid_count,
                message=f"Load completed: {stats['inserted']} inserted, {stats['updated']} updated, {invalid_count} invalid"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise ETLLoadError(f"Data load failed: {str(e)}")


===FILE: src/logger.py===
"""
ETL Logger Module
Implements logging functionality for ETL operations.
Migrated from ABAP ZCL_ETL_LOGGER class.
"""

from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """
    Logger for ETL process tracking and audit trail.
    Implements functionality from ABAP ZCL_ETL_LOGGER.
    """
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        """
        Initialize ETL logger with run ID.
        
        Args:
            etl_run_id: Unique identifier for ETL execution
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(f"ETL_{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
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
    ) -> None:
        """
        Log ETL message with statistics.
        Implements ABAP log_message method.
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Failed records
        """
        log_entry = {
            'etl_run_id': self.etl_run_id,
            'timestamp': datetime.now().isoformat(),
            'step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        # Map status to log level
        if status == 'E':
            self.logger.error(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Error: {records_error}")
        elif status == 'W':
            self.logger.warning(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Error: {records_error}")
        elif status == 'S':
            self.logger.info(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Error: {records_error}")
        else:
            self.logger.debug(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Error: {records_error}")
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id


===FILE: src/exceptions.py===
"""
ETL Exception Classes
Custom exceptions for ETL error handling.
Migrated from ABAP ZCX_ETL_ERROR class.
"""


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(self, message: str, error_step: str = None, record_id: str = None):
        super().__init__(message)
        self.error_step = error_step
        self.record_id = record_id


class ETLExtractError(ETLError):
    """Exception raised during data extraction."""
    pass


class ETLTransformError(ETLError):
    """Exception raised during data transformation."""
    pass


class ETLLoadError(ETLError):
    """Exception raised during data loading."""
    pass


class ETLValidationError(ETLError):
    """Exception raised during data validation."""
    pass


===FILE: config.yaml===
# ETL Configuration
# Migrated from ABAP ZCL_ETL_CONSTANTS and package configuration

etl:
  # Application metadata
  app_name: "Sales ETL System"
  version: "1.0.0"
  
  # Spark configuration
  spark:
    app_name: "SalesETL"
    master: "local[*]"
    config:
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
      spark.sql.sources.partitionOverwriteMode: "dynamic"
      spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
      spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  
  # Database configuration
  database:
    format: "delta"  # parquet, delta, or jdbc
    source_table: "sales_raw"
    target_table: "sales_analytics"
    log_table: "etl_log"
    
  # Processing configuration
  processing:
    batch_size: 1000
    commit_interval: 500
    retry_attempts: 3
    timeout_seconds: 3600
    enable_validation: true
    parallel_jobs: 4
    
  # Status codes (from ABAP gc_status)
  status_codes:
    new: "N"
    processed: "P"
    error: "E"
    warning: "W"
    success: "S"
    info: "I"
    
  # Process steps (from ABAP gc_step)
  process_steps:
    init: "INIT"
    extract: "EXTRACT"
    transform: "TRANSFORM"
    load: "LOAD"
    validate: "VALIDATE"
    complete: "COMPLETE"
    error: "ERROR"
    
  # Business rules - Categories (from ABAP gc_category)
  categories:
    high: "HIGH"
    medium: "MEDIUM"
    low: "LOW"
    
  # Business rules - Discount thresholds
  discount:
    quantity_tier1: 10
    quantity_tier2: 15
    rate_tier1: 0.05
    rate_tier2: 0.10
    
  # Business rules - Tax and cost
  tax_rate: 0.08
  cost_ratio: 0.60
  
  # Business rules - Category thresholds
  category_thresholds:
    high: 2000.00
    medium: 500.00
    
  # ID prefixes
  prefixes:
    etl_run: "ETL"
    log_id: "LOG"
    analytics_id: "ANL"
    
  # Logging configuration
  logging:
    level: "INFO"
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
  # User information
  user: "spark_etl_system"


===FILE: tests/test_load.py===
"""
Unit tests for DataLoader class.
Tests upsert logic, validation, and error handling.
"""

import pytest
from datetime import date, datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.load import DataLoader
from src.logger import ETLLogger
from src.exceptions import ETLLoadError


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("TestDataLoader") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def logger():
    """Create test logger."""
    return ETLLogger("TEST_RUN_001", "DEBUG")


@pytest.fixture
def config():
    """Test configuration."""
    return {
        'target_table': 'test_sales_analytics',
        'source_table': 'test_sales_raw',
        'format': 'parquet',
        'batch_size': 100,
        'enable_validation': True,
        'user': 'test_user'
    }


@pytest.fixture
def sample_analytics_data(spark):
    """Create sample analytics data for testing."""
    schema = StructType([
        StructField("analytics_id", StringType(), False),
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
    
    data = [
        ("ANL001", date(2024, 1, 15), "CUST001", "PROD001", 10, 
         Decimal("999.90"), Decimal("1029.89"), Decimal("49.99"), Decimal("79.99"),
         "USD", "John Doe", "NORTH", Decimal("35.50"), "HIGH", "ETL001"),
        ("ANL002", date(2024, 1, 15), "CUST002", "PROD002", 5,
         Decimal("749.95"), Decimal("774.35"), Decimal("0.00"), Decimal("59.99"),
         "USD", "Jane Smith", "SOUTH", Decimal("40.20"), "MEDIUM", "ETL001"),
        ("ANL003", date(2024, 1, 15), "CUST003", "PROD001", 20,
         Decimal("1999.80"), Decimal("2015.80"), Decimal("199.98"), Decimal("144.00"),
         "USD", "John Doe", "EAST", Decimal("38.75"), "HIGH", "ETL001")
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def invalid_analytics_data(spark):
    """Create invalid analytics data for validation testing."""
    schema = StructType([
        StructField("analytics_id", StringType(), True),
        StructField("trans_date", DateType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("total_quantity", IntegerType(), True),
        StructField("gross_amount", DecimalType(16, 2), True),
        StructField("net_amount", DecimalType(16, 2), True),
        StructField("discount_amount", DecimalType(16, 2), True),
        StructField("tax_amount", DecimalType(16, 2), True),
        StructField("currency", StringType(), True),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("profit_margin", DecimalType(5, 2), True),
        StructField("category", StringType(), True),
        StructField("etl_run_id", StringType(), True)
    ])
    
    data = [
        (None, date(2024, 1, 15), "CUST001", "PROD001", 10, 
         Decimal("999.90"), Decimal("1029.89"), Decimal("49.99"), Decimal("79.99"),
         "USD", "John Doe", "NORTH", Decimal("35.50"), "HIGH", "ETL001"),  # Missing analytics_id
        ("ANL004", date(2024, 1, 15), None, "PROD002", 5,
         Decimal("749.95"), Decimal("774.35"), Decimal("0.00"), Decimal("59.99"),
         "USD", "Jane Smith", "SOUTH", Decimal("40.20"), "MEDIUM", "ETL001"),  # Missing customer_id
        ("ANL005", date(2024, 1, 15), "CUST003", "PROD001", 20,
         Decimal("-100.00"), Decimal("2015.80"), Decimal("199.98"), Decimal("144.00"),
         "USD", "John Doe", "EAST", Decimal("38.75"), "INVALID", "ETL001")  # Invalid amount and category
    ]
    
    return spark.createDataFrame(data, schema)


class TestDataLoader:
    """Test suite for DataLoader class."""
    
    def test_initialization(self, spark, logger, config):
        """Test DataLoader initialization."""
        loader = DataLoader(spark, logger, config)
        
        assert loader.spark == spark
        assert loader.logger == logger
        assert loader.target_table == 'test_sales_analytics'
        assert loader.batch_size == 100
        assert loader.enable_validation is True
    
    def test_get_target_schema(self, spark, logger, config):
        """Test target schema definition."""
        loader = DataLoader(spark, logger, config)
        schema = loader.get_target_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 18
        assert schema.fieldNames()[0] == "analytics_id"
        assert schema["analytics_id"].nullable is False
        assert schema["gross_amount"].dataType.typeName() == "decimal(16,2)"
    
    def test_validate_record_valid_data(self, spark, logger, config, sample_analytics_data):
        """Test validation with valid records."""
        loader = DataLoader(spark, logger, config)
        validated_df = loader.validate_record(sample_analytics_data)
        
        # Check that is_valid column was added
        assert "is_valid" in validated_df.columns
        
        # All records should be valid
        valid_count = validated_df.filter("is_valid = true").count()
        assert valid_count == 3
    
    def test_validate_record_invalid_data(self, spark, logger, config, invalid_analytics_data):
        """Test validation with invalid records."""
        loader = DataLoader(spark, logger, config)
        validated_df = loader.validate_record(invalid_analytics_data)
        
        # Check validation results
        invalid_count = validated_df.filter("is_valid = false").count()
        assert invalid_count == 3
        
        # Check validation messages
        assert "validation_message" in validated_df.columns
        messages = validated_df.filter("is_valid = false").select("validation_message").collect()
        assert all(row.validation_message is not None for row in messages)
    
    def test_validate_record_disabled(self, spark, logger, sample_analytics_data):
        """Test validation when disabled."""
        config = {
            'target_table': 'test_sales_analytics',
            'enable_validation': False,
            'user': 'test_user'
        }
        loader = DataLoader(spark, logger, config)
        validated_df = loader.validate_record(sample_analytics_data)
        
        # All records should be marked as valid when validation is disabled
        valid_count = validated_df.filter("is_valid = true").count()
        assert valid_count == sample_analytics_data.count()
    
    def test_prepare_upsert_data_no_existing_table(self, spark, logger, config, sample_analytics_data):
        """Test prepare_upsert_data when target table doesn't exist."""
        loader = DataLoader(spark, logger, config)
        insert_df, update_df = loader.prepare_upsert_data(sample_analytics_data)
        
        # All records should be inserts
        assert insert_df.count() == 3
        assert update_df.count() == 0
        
        # Check metadata columns were added
        assert "loaded_at" in insert_df.columns
        assert "loaded_by" in insert_df.columns
        assert insert_df.select("loaded_by").first()[0] == "test_user"
    
    def test_prepare_upsert_data_with_existing_records(self, spark, logger, config, sample_analytics_data):
        """Test prepare_upsert_data with existing records."""
        loader = DataLoader(spark, logger, config)
        
        # Create target table with one existing record
        existing_data = sample_analytics_data.limit(1)
        existing_data.write.format("parquet").mode("overwrite").saveAsTable(config['target_table'])
        
        try:
            insert_df, update_df = loader.prepare_upsert_data(sample_analytics_data)
            
            # Should have 1 update and 2 inserts
            assert update_df.count() == 1
            assert insert_df.count() == 2
            
            # Update records should have updated_at and updated_by
            assert "updated_at" in update_df.columns
            assert "updated_by" in update_df.columns
            
        finally: