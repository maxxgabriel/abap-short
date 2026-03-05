===FILE: src/load.py===
"""
PySpark Data Loader Module
Loads transformed analytics data with INSERT and UPDATE operations (upsert logic)
Migrated from ZCL_ETL_LOADER ABAP class
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Dict, Tuple
import yaml
from datetime import datetime

from src.logger import ETLLogger


class DataLoader:
    """
    Loads transformed analytics data into target table with upsert logic.
    Supports both INSERT (new records) and UPDATE (existing records) operations.
    """
    
    def __init__(self, spark: SparkSession, config: Dict, logger: ETLLogger):
        """
        Initialize DataLoader with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.target_table = config['load']['target_table']
        self.source_status_table = config['load']['source_status_table']
        self.batch_size = config['load'].get('batch_size', 1000)
        self.upsert_mode = config['load'].get('upsert_mode', 'merge')  # 'merge' or 'overwrite'
        
    def get_target_schema(self) -> StructType:
        """
        Define schema for analytics target table.
        
        Returns:
            StructType: Schema definition
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
    
    def validate_record(self, row: Dict) -> Tuple[bool, str]:
        """
        Validate individual analytics record.
        Migrated from validate_record method in ABAP.
        
        Args:
            row: Dictionary representing a record
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate required fields
        if not row.get('analytics_id'):
            return False, "Missing analytics_id"
        
        if not row.get('customer_id'):
            return False, "Missing customer_id"
        
        if not row.get('product_id'):
            return False, "Missing product_id"
        
        # Validate gross amount
        gross_amount = row.get('gross_amount', 0)
        if gross_amount <= 0:
            return False, f"Invalid gross_amount: {gross_amount}"
        
        # Validate currency
        if not row.get('currency'):
            return False, "Missing currency"
        
        # Validate category
        category = row.get('category', '')
        if category not in ['HIGH', 'MEDIUM', 'LOW']:
            return False, f"Invalid category: {category}"
        
        return True, ""
    
    def validate_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Validate entire DataFrame and filter valid records.
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            DataFrame with only valid records and validation status
        """
        # Add validation columns
        df_with_validation = df.withColumn(
            "is_valid",
            F.when(
                (F.col("analytics_id").isNotNull()) &
                (F.col("customer_id").isNotNull()) &
                (F.col("product_id").isNotNull()) &
                (F.col("gross_amount") > 0) &
                (F.col("currency").isNotNull()) &
                (F.col("category").isin(['HIGH', 'MEDIUM', 'LOW'])),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        
        # Log validation results
        total_count = df_with_validation.count()
        valid_count = df_with_validation.filter(F.col("is_valid") == True).count()
        invalid_count = total_count - valid_count
        
        if invalid_count > 0:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Found {invalid_count} invalid records out of {total_count}"
            )
        
        return df_with_validation
    
    def prepare_for_insert(self, df: DataFrame) -> DataFrame:
        """
        Prepare DataFrame for INSERT operation.
        Add metadata fields for new records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with INSERT metadata
        """
        current_timestamp = F.current_timestamp()
        current_user = F.lit(self.config.get('runtime', {}).get('user', 'etl_system'))
        
        return df.withColumn("loaded_at", current_timestamp) \
                 .withColumn("loaded_by", current_user) \
                 .withColumn("updated_at", F.lit(None).cast(TimestampType())) \
                 .withColumn("updated_by", F.lit(None).cast(StringType()))
    
    def prepare_for_update(self, df: DataFrame) -> DataFrame:
        """
        Prepare DataFrame for UPDATE operation.
        Add metadata fields for updated records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with UPDATE metadata
        """
        current_timestamp = F.current_timestamp()
        current_user = F.lit(self.config.get('runtime', {}).get('user', 'etl_system'))
        
        return df.withColumn("updated_at", current_timestamp) \
                 .withColumn("updated_by", current_user)
    
    def identify_insert_update_records(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Identify which records need INSERT vs UPDATE operations.
        Uses analytics_id as primary key to check existing records.
        
        Args:
            df: Input DataFrame with new/updated records
            
        Returns:
            Tuple of (insert_df, update_df)
        """
        try:
            # Read existing target table
            existing_df = self.spark.read.format(self.config['load']['format']) \
                                   .load(self.target_table) \
                                   .select("analytics_id", "loaded_at")
            
            # Left anti join to find records that don't exist (INSERT)
            insert_df = df.join(
                existing_df,
                on="analytics_id",
                how="left_anti"
            )
            
            # Inner join to find records that exist (UPDATE)
            update_df = df.join(
                existing_df,
                on="analytics_id",
                how="inner"
            ).drop(existing_df["loaded_at"])
            
            insert_count = insert_df.count()
            update_count = update_df.count()
            
            self.logger.log_message(
                step='LOAD',
                status='I',
                message=f"Identified {insert_count} records for INSERT, {update_count} for UPDATE"
            )
            
            return insert_df, update_df
            
        except Exception as e:
            # If table doesn't exist, all records are for INSERT
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Target table not found, treating all records as INSERT: {str(e)}"
            )
            return df, self.spark.createDataFrame([], df.schema)
    
    def perform_upsert_merge(self, df: DataFrame) -> bool:
        """
        Perform upsert using Delta Lake MERGE operation.
        Most efficient approach for Delta tables.
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Success status
        """
        try:
            from delta.tables import DeltaTable
            
            # Check if target table exists
            if DeltaTable.isDeltaTable(self.spark, self.target_table):
                delta_table = DeltaTable.forPath(self.spark, self.target_table)
                
                # Prepare source with update metadata
                source_df = self.prepare_for_update(df)
                
                # Perform MERGE operation
                delta_table.alias("target").merge(
                    source_df.alias("source"),
                    "target.analytics_id = source.analytics_id"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    message=f"MERGE operation completed successfully"
                )
                return True
            else:
                # Table doesn't exist, perform initial INSERT
                prepared_df = self.prepare_for_insert(df)
                prepared_df.write.format("delta") \
                          .mode("overwrite") \
                          .save(self.target_table)
                
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    message=f"Initial load completed (table created)"
                )
                return True
                
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"MERGE operation failed: {str(e)}"
            )
            return False
    
    def perform_upsert_separate(self, df: DataFrame) -> bool:
        """
        Perform upsert using separate INSERT and UPDATE operations.
        Works with non-Delta formats.
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Success status
        """
        try:
            # Identify INSERT vs UPDATE records
            insert_df, update_df = self.identify_insert_update_records(df)
            
            insert_count = 0
            update_count = 0
            
            # Handle INSERT records
            if insert_df.count() > 0:
                prepared_insert = self.prepare_for_insert(insert_df)
                prepared_insert.write.format(self.config['load']['format']) \
                              .mode("append") \
                              .save(self.target_table)
                insert_count = prepared_insert.count()
                
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    records_success=insert_count,
                    message=f"Inserted {insert_count} new records"
                )
            
            # Handle UPDATE records
            if update_df.count() > 0:
                prepared_update = self.prepare_for_update(update_df)
                
                # Read full table, update matching records, write back
                existing_full = self.spark.read.format(self.config['load']['format']) \
                                         .load(self.target_table)
                
                # Drop old versions of updated records
                non_updated = existing_full.join(
                    prepared_update.select("analytics_id"),
                    on="analytics_id",
                    how="left_anti"
                )
                
                # Combine non-updated records with updated records
                final_df = non_updated.union(prepared_update)
                
                # Write back with overwrite
                final_df.write.format(self.config['load']['format']) \
                       .mode("overwrite") \
                       .save(self.target_table)
                
                update_count = prepared_update.count()
                
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    records_success=update_count,
                    message=f"Updated {update_count} existing records"
                )
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=insert_count + update_count,
                records_success=insert_count + update_count,
                message=f"Upsert completed: {insert_count} inserts, {update_count} updates"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"Upsert operation failed: {str(e)}"
            )
            return False
    
    def update_source_status(self, processed_ids: list) -> bool:
        """
        Update status in source raw table after successful load.
        Migrated from source status update logic in ABAP.
        
        Args:
            processed_ids: List of transaction IDs that were processed
            
        Returns:
            Success status
        """
        try:
            if not processed_ids:
                return True
            
            # Read source table
            source_df = self.spark.read.format(self.config['extract']['format']) \
                                  .load(self.source_status_table)
            
            # Update status to 'P' (Processed) for loaded records
            updated_df = source_df.withColumn(
                "status",
                F.when(
                    F.col("trans_id").isin(processed_ids),
                    F.lit('P')
                ).otherwise(F.col("status"))
            )
            
            # Write back
            updated_df.write.format(self.config['extract']['format']) \
                     .mode("overwrite") \
                     .save(self.source_status_table)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f"Updated status for {len(processed_ids)} source records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Failed to update source status: {str(e)}"
            )
            return False
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Main load method - orchestrates validation and upsert operations.
        Migrated from load_data method in ABAP ZCL_ETL_LOADER.
        
        Args:
            analytics_df: Transformed analytics DataFrame to load
            
        Returns:
            Success status
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            total_count = analytics_df.count()
            
            if total_count == 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message='No records to load'
                )
                return True
            
            # Validate records
            validated_df = self.validate_dataframe(analytics_df)
            valid_df = validated_df.filter(F.col("is_valid") == True).drop("is_valid")
            invalid_df = validated_df.filter(F.col("is_valid") == False)
            
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            
            if invalid_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    records_error=invalid_count,
                    message=f'Skipping {invalid_count} invalid records'
                )
            
            if valid_count == 0:
                self.logger.log_message(
                    step='LOAD',
                    status='E',
                    message='No valid records to load'
                )
                return False
            
            # Perform upsert based on configuration
            if self.upsert_mode == 'merge' and self.config['load']['format'] == 'delta':
                success = self.perform_upsert_merge(valid_df)
            else:
                success = self.perform_upsert_separate(valid_df)
            
            if not success:
                return False
            
            # Update source table status
            trans_ids = [row['trans_id'] for row in valid_df.select("trans_id").distinct().collect()]
            self.update_source_status(trans_ids)
            
            # Log final statistics
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=total_count,
                records_success=valid_count,
                records_error=invalid_count,
                message=f'Loaded {valid_count} of {total_count} records'
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            return False


def create_loader(spark: SparkSession, config_path: str = "config.yaml") -> DataLoader:
    """
    Factory function to create DataLoader instance.
    
    Args:
        spark: Active SparkSession
        config_path: Path to configuration file
        
    Returns:
        Configured DataLoader instance
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    from src.logger import create_logger
    logger = create_logger(config)
    
    return DataLoader(spark, config, logger)


===FILE: src/logger.py===
"""
ETL Logger Module
Provides structured logging for ETL operations
Migrated from ZCL_ETL_LOGGER ABAP class
"""

from typing import Optional
from datetime import datetime
import uuid
import yaml


class ETLLogger:
    """
    Logger for ETL process with structured logging support.
    """
    
    # Status codes
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Process steps
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, config: dict):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.etl_run_id = etl_run_id
        self.config = config
        self.logs = []
    
    def generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG{timestamp}{str(uuid.uuid4())[:6]}"
    
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
        Log a message with metadata.
        
        Args:
            step: ETL process step
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            'log_id': self.generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_date': datetime.now().strftime('%Y-%m-%d'),
            'execution_time': datetime.now().strftime('%H:%M:%S'),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        self.logs.append(log_entry)
        
        # Console output
        status_symbol = {
            'S': '✓',
            'E': '✗',
            'W': '⚠',
            'I': 'ℹ'
        }.get(status, '•')
        
        print(f"[{log_entry['execution_time']}] {status_symbol} {step}: {message}")
        
        if records_processed > 0:
            print(f"  → Processed: {records_processed}, Success: {records_success}, Errors: {records_error}")
    
    def get_etl_run_id(self) -> str:
        """
        Get ETL run ID.
        
        Returns:
            ETL run ID
        """
        return self.etl_run_id
    
    def get_logs(self) -> list:
        """
        Get all log entries.
        
        Returns:
            List of log entries
        """
        return self.logs


def create_logger(config: dict) -> ETLLogger:
    """
    Factory function to create logger instance.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        ETLLogger instance
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    etl_run_id = f"ETL{timestamp}"
    return ETLLogger(etl_run_id, config)


===FILE: config.yaml===
# ETL Configuration
# Migrated from ZCL_ETL_CONSTANTS and ABAP configuration

# Data source configuration
extract:
  format: "parquet"  # or "delta", "csv", "jdbc"
  source_table: "data/raw/sales_raw"
  date_column: "trans_date"
  status_column: "status"
  new_status: "N"
  processed_status: "P"
  batch_size: 1000

# Transformation configuration
transform:
  # Business rules - Discount thresholds
  discount:
    quantity_tier1: 10
    quantity_tier2: 15
    rate_tier1: 0.05
    rate_tier2: 0.10
  
  # Tax rate
  tax_rate: 0.08
  
  # Cost ratio for profit calculation
  cost_ratio: 0.60
  
  # Category thresholds
  category:
    high_threshold: 2000.00
    medium_threshold: 500.00

# Load configuration
load:
  format: "delta"  # "delta", "parquet", "jdbc"
  target_table: "data/analytics/sales_analytics"
  source_status_table: "data/raw/sales_raw"
  batch_size: 1000
  upsert_mode: "merge"  # "merge" (Delta only) or "separate"
  
  # Primary key for upsert
  primary_key: "analytics_id"
  
  # Validation rules
  validation:
    required_fields:
      - "analytics_id"
      - "customer_id"
      - "product_id"
      - "gross_amount"
      - "currency"
    valid_categories:
      - "HIGH"
      - "MEDIUM"
      - "LOW"

# ETL configuration defaults
etl:
  default_batch_size: 1000
  default_commit_interval: 500
  default_retry_attempts: 3
  default_timeout_seconds: 3600
  
  # ID prefixes
  prefix:
    etl_run: "ETL"
    log_id: "LOG"
    analytics_id: "ANL"

# Runtime configuration
runtime:
  user: "etl_system"
  application: "sales_etl"
  
  # Spark configuration
  spark:
    app_name: "Sales ETL Pipeline"
    master: "local[*]"
    log_level: "WARN"
    
    # Delta Lake extensions
    extensions:
      - "io.delta.sql.DeltaSparkSessionExtension"
    
    config:
      spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
      spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"

# Logging configuration
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  output_table: "data/logs/etl_log"
  console_output: true

# Message texts
messages:
  init_success: "ETL process initialized successfully"
  extract_start: "Starting data extraction"
  extract_complete: "Data extraction completed"
  transform_start: "Starting data transformation"
  transform_complete: "Data transformation completed"
  load_start: "Starting data load"
  load_complete: "Data load completed"
  etl_complete: "ETL process completed successfully"
  etl_error: "ETL process failed"


===FILE: tests/test_load.py===
"""
Unit tests for DataLoader module
Tests INSERT, UPDATE, and upsert logic
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import date, datetime
import tempfile
import shutil
import os

from src.load import DataLoader
from src.logger import ETLLogger


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("test_loader") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create temporary directory for test data."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def config(temp_dir):
    """Create test configuration."""
    return {
        'extract': {
            'format': 'delta',
            'source_table': f"{temp_dir}/raw"
        },
        'load': {
            'format': 'delta',
            'target_table': f"{temp_dir}/analytics",
            'source_status_table': f"{temp_dir}/raw",
            'batch_size': 100,
            'upsert_mode': 'merge',
            'primary_key': 'analytics_id',
            'validation': {
                'required_fields': ['analytics_id', 'customer_id', 'product_id', 'gross_amount', 'currency'],
                'valid_categories': ['HIGH', 'MEDIUM', 'LOW']
            }
        },
        'runtime': {
            'user': 'test_user'
        }
    }


@pytest.fixture
def logger(config):
    """Create test logger."""
    return ETLLogger("TEST_RUN_001", config)


@pytest.fixture
def loader(spark, config, logger):
    """Create DataLoader instance."""
    return DataLoader(spark, config, logger)


@pytest.fixture
def sample_analytics_data(spark):
    """Create sample analytics data for testing."""
    data = [
        ("ANL001", date(2024, 1, 1), "CUST001", "PROD001", 10, 999.90, 949.91, 49.99, 71.99, 
         "USD", "John Doe", "NORTH", 25.5, "MEDIUM", "ETL001", "T000001"),
        ("ANL002", date(2024, 1, 1), "CUST002", "PROD002", 5, 749.95, 712.45, 37.50, 53.98,
         "USD", "Jane Smith", "SOUTH", 28.3, "MEDIUM", "ETL001", "T000002"),
        ("ANL003", date(2024, 1, 1), "CUST003", "PROD001", 20, 1999.80, 1819.82, 199.98, 145.59,
         "USD", "John Doe", "EAST", 30.1, "MEDIUM", "ETL001", "T000003")
    ]
    
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
        StructField("etl_run_id", StringType(), False),
        StructField("trans_id", StringType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


class TestDataLoader:
    """Test suite for DataLoader class."""
    
    def test_validate_record_valid(self, loader):
        """Test validation of valid record."""
        valid_record = {
            'analytics_id': 'ANL001',
            'customer_id': 'CUST001',
            'product_id': 'PROD001',
            'gross_amount': 100.50,
            'currency': 'USD',
            'category': 'MEDIUM'
        }
        
        is_valid, error_msg = loader.validate_record(valid_record)
        assert is_valid == True
        assert error_msg == ""
    
    def test_validate_record_missing_id(self, loader):
        """Test validation with missing analytics_id."""
        invalid_record = {
            'analytics_id': None,
            'customer_id': 'CUST001',
            'product_id': 'PROD001',
            'gross_amount': 100.50,
            'currency': 'USD',
            'category': 'MEDIUM'
        }
        
        is_valid, error_msg = loader.validate_record(invalid_record)
        assert is_valid == False
        assert "analytics_id" in error_msg
    
    def test_validate_record_invalid_amount(self, loader):
        """Test validation with invalid gross amount."""
        invalid_record = {
            'analytics_id': 'ANL001',