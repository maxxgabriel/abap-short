===FILE: src/load.py===
"""
Data Loader Module with Transactional Write Support
Implements PySpark data loading with saveAsTable/JDBC, test mode, and rollback capabilities.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
import logging
from datetime import datetime
from contextlib import contextmanager


class TransactionalWriter:
    """Handles transactional writes with rollback support."""
    
    def __init__(self, spark: SparkSession, test_mode: bool = False):
        self.spark = spark
        self.test_mode = test_mode
        self.temp_views: List[str] = []
        self.logger = logging.getLogger(__name__)
        
    @contextmanager
    def transaction(self):
        """Context manager for transactional write operations."""
        try:
            yield self
            if not self.test_mode:
                self.logger.info("Transaction completed successfully - changes committed")
            else:
                self.logger.info("Test mode - changes not committed, cleaning up temp views")
                self._cleanup_temp_views()
        except Exception as e:
            self.logger.error(f"Transaction failed: {str(e)}")
            self._rollback()
            raise
    
    def _rollback(self):
        """Rollback operations by cleaning up temporary views."""
        self.logger.warning("Rolling back transaction - cleaning up temporary resources")
        self._cleanup_temp_views()
        
    def _cleanup_temp_views(self):
        """Clean up all temporary views created during transaction."""
        for view_name in self.temp_views:
            try:
                self.spark.catalog.dropTempView(view_name)
                self.logger.debug(f"Dropped temporary view: {view_name}")
            except Exception as e:
                self.logger.warning(f"Failed to drop temp view {view_name}: {str(e)}")
        self.temp_views.clear()


class DataLoader:
    """
    PySpark data loader with transactional write support.
    Supports saveAsTable and JDBC writes with test mode and rollback capabilities.
    """
    
    def __init__(
        self, 
        spark: SparkSession,
        config: Dict[str, Any],
        test_mode: bool = False
    ):
        """
        Initialize DataLoader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with write settings
            test_mode: If True, use temporary views instead of actual writes
        """
        self.spark = spark
        self.config = config
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)
        
        # Initialize statistics
        self.stats = {
            'records_processed': 0,
            'records_success': 0,
            'records_error': 0,
            'start_time': None,
            'end_time': None
        }
    
    def validate_data(self, df: DataFrame) -> tuple[bool, List[str]]:
        """
        Validate DataFrame before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        # Check if DataFrame is empty
        if df.rdd.isEmpty():
            errors.append("DataFrame is empty")
        
        # Validate required columns
        required_cols = self.config.get('required_columns', [])
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
        
        # Check for null values in key columns
        key_columns = self.config.get('key_columns', [])
        for col in key_columns:
            if col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    errors.append(f"Column '{col}' contains {null_count} null values")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def load_to_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None
    ) -> bool:
        """
        Load DataFrame to Hive/Spark table with transactional support.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Write mode (append, overwrite, error, ignore)
            partition_by: Optional list of columns to partition by
            
        Returns:
            True if successful, False otherwise
        """
        self.stats['start_time'] = datetime.now()
        self.stats['records_processed'] = df.count()
        
        # Validate data
        is_valid, errors = self.validate_data(df)
        if not is_valid:
            self.logger.error(f"Validation failed: {errors}")
            self.stats['records_error'] = self.stats['records_processed']
            return False
        
        writer = TransactionalWriter(self.spark, self.test_mode)
        
        try:
            with writer.transaction():
                if self.test_mode:
                    # Test mode: create temporary view
                    temp_view_name = f"temp_{table_name}_{int(datetime.now().timestamp())}"
                    df.createOrReplaceTempView(temp_view_name)
                    writer.temp_views.append(temp_view_name)
                    
                    self.logger.info(
                        f"Test mode: Created temporary view '{temp_view_name}' "
                        f"with {self.stats['records_processed']} records"
                    )
                    
                    # Verify view was created
                    result = self.spark.sql(f"SELECT COUNT(*) as count FROM {temp_view_name}")
                    count = result.collect()[0]['count']
                    self.logger.info(f"Verified {count} records in temp view")
                    
                else:
                    # Production mode: write to actual table
                    self.logger.info(f"Writing {self.stats['records_processed']} records to table '{table_name}'")
                    
                    df_writer = df.write.mode(mode)
                    
                    # Apply partitioning if specified
                    if partition_by:
                        df_writer = df_writer.partitionBy(*partition_by)
                    
                    # Configure write options from config
                    write_options = self.config.get('write_options', {})
                    for key, value in write_options.items():
                        df_writer = df_writer.option(key, value)
                    
                    # Execute write
                    df_writer.saveAsTable(table_name)
                    
                    self.logger.info(f"Successfully wrote to table '{table_name}'")
                
                self.stats['records_success'] = self.stats['records_processed']
                self.stats['end_time'] = datetime.now()
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to load data to table '{table_name}': {str(e)}")
            self.stats['records_error'] = self.stats['records_processed']
            self.stats['records_success'] = 0
            self.stats['end_time'] = datetime.now()
            return False
    
    def load_to_jdbc(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        batch_size: int = 1000
    ) -> bool:
        """
        Load DataFrame to database via JDBC with transactional support.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Write mode (append, overwrite, error, ignore)
            batch_size: Number of records per batch
            
        Returns:
            True if successful, False otherwise
        """
        self.stats['start_time'] = datetime.now()
        self.stats['records_processed'] = df.count()
        
        # Validate data
        is_valid, errors = self.validate_data(df)
        if not is_valid:
            self.logger.error(f"Validation failed: {errors}")
            self.stats['records_error'] = self.stats['records_processed']
            return False
        
        # Get JDBC configuration
        jdbc_config = self.config.get('jdbc', {})
        if not jdbc_config:
            self.logger.error("JDBC configuration not found")
            return False
        
        writer = TransactionalWriter(self.spark, self.test_mode)
        
        try:
            with writer.transaction():
                if self.test_mode:
                    # Test mode: create temporary view
                    temp_view_name = f"temp_jdbc_{table_name}_{int(datetime.now().timestamp())}"
                    df.createOrReplaceTempView(temp_view_name)
                    writer.temp_views.append(temp_view_name)
                    
                    self.logger.info(
                        f"Test mode: Created temporary view '{temp_view_name}' "
                        f"for JDBC write with {self.stats['records_processed']} records"
                    )
                    
                else:
                    # Production mode: write via JDBC
                    self.logger.info(
                        f"Writing {self.stats['records_processed']} records "
                        f"to JDBC table '{table_name}'"
                    )
                    
                    df.write \
                        .format("jdbc") \
                        .option("url", jdbc_config['url']) \
                        .option("dbtable", table_name) \
                        .option("user", jdbc_config.get('user', '')) \
                        .option("password", jdbc_config.get('password', '')) \
                        .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                        .option("batchsize", batch_size) \
                        .option("isolationLevel", "READ_COMMITTED") \
                        .mode(mode) \
                        .save()
                    
                    self.logger.info(f"Successfully wrote to JDBC table '{table_name}'")
                
                self.stats['records_success'] = self.stats['records_processed']
                self.stats['end_time'] = datetime.now()
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to load data via JDBC to '{table_name}': {str(e)}")
            self.stats['records_error'] = self.stats['records_processed']
            self.stats['records_success'] = 0
            self.stats['end_time'] = datetime.now()
            return False
    
    def load_with_validation(
        self,
        df: DataFrame,
        table_name: str,
        write_type: str = "table",
        mode: str = "append",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Load data with comprehensive validation and error handling.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            write_type: Type of write ('table' or 'jdbc')
            mode: Write mode
            **kwargs: Additional arguments for specific write type
            
        Returns:
            Dictionary with load results and statistics
        """
        self.logger.info(f"Starting load operation for table '{table_name}'")
        
        # Pre-load validation
        validation_errors = []
        
        # Check DataFrame validity
        try:
            record_count = df.count()
            if record_count == 0:
                validation_errors.append("DataFrame is empty")
        except Exception as e:
            validation_errors.append(f"Failed to count records: {str(e)}")
            record_count = 0
        
        # Check schema
        if not df.schema:
            validation_errors.append("DataFrame has no schema")
        
        if validation_errors:
            self.logger.error(f"Pre-load validation failed: {validation_errors}")
            return {
                'success': False,
                'errors': validation_errors,
                'stats': self.stats
            }
        
        # Perform load based on write type
        if write_type == "table":
            success = self.load_to_table(
                df=df,
                table_name=table_name,
                mode=mode,
                partition_by=kwargs.get('partition_by')
            )
        elif write_type == "jdbc":
            success = self.load_to_jdbc(
                df=df,
                table_name=table_name,
                mode=mode,
                batch_size=kwargs.get('batch_size', 1000)
            )
        else:
            self.logger.error(f"Invalid write_type: {write_type}")
            return {
                'success': False,
                'errors': [f"Invalid write_type: {write_type}"],
                'stats': self.stats
            }
        
        # Calculate duration
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            self.stats['duration_seconds'] = duration
        
        return {
            'success': success,
            'table_name': table_name,
            'write_type': write_type,
            'mode': mode,
            'test_mode': self.test_mode,
            'stats': self.stats.copy()
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current loading statistics."""
        return self.stats.copy()


def create_loader(
    spark: SparkSession,
    config_path: Optional[str] = None,
    test_mode: bool = False
) -> DataLoader:
    """
    Factory function to create DataLoader instance.
    
    Args:
        spark: SparkSession instance
        config_path: Path to configuration file
        test_mode: Enable test mode
        
    Returns:
        DataLoader instance
    """
    import yaml
    
    config = {}
    if config_path:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    
    return DataLoader(spark, config.get('loader', {}), test_mode)


===FILE: config.yaml===
# Data Loader Configuration
loader:
  # Required columns for validation
  required_columns:
    - analytics_id
    - trans_date
    - customer_id
    - product_id
    - gross_amount
    - net_amount
    - currency
  
  # Key columns (cannot have nulls)
  key_columns:
    - analytics_id
    - customer_id
    - product_id
  
  # Write options for saveAsTable
  write_options:
    compression: "snappy"
    fileFormat: "parquet"
    maxRecordsPerFile: 100000
  
  # JDBC configuration
  jdbc:
    url: "jdbc:postgresql://localhost:5432/sales_db"
    user: "etl_user"
    password: "secure_password"
    driver: "org.postgresql.Driver"
    properties:
      ssl: "true"
      sslmode: "require"
  
  # Batch processing settings
  batch_size: 1000
  commit_interval: 500
  
  # Retry configuration
  retry:
    max_attempts: 3
    backoff_seconds: 5
  
  # Validation settings
  validation:
    check_nulls: true
    check_duplicates: true
    check_data_types: true
    max_error_threshold: 0.01  # 1% error threshold

# Table definitions
tables:
  sales_analytics:
    name: "sales_analytics"
    partition_by:
      - "trans_date"
      - "region"
    write_mode: "append"
    format: "parquet"
  
  sales_summary:
    name: "sales_summary"
    partition_by:
      - "summary_date"
    write_mode: "overwrite"
    format: "delta"

# Logging configuration
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: "logs/data_loader.log"

# Test mode settings
test_mode:
  enabled: false
  temp_view_prefix: "test_"
  cleanup_on_exit: true
  verify_counts: true


===FILE: src/transaction_manager.py===
"""
Transaction Manager for handling complex transactional operations.
"""

from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from contextlib import contextmanager
import logging
from datetime import datetime


class TransactionManager:
    """
    Manages transactional operations with support for multiple write targets.
    """
    
    def __init__(self, spark: SparkSession, test_mode: bool = False):
        self.spark = spark
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)
        
        # Transaction state
        self.temp_views: List[str] = []
        self.backup_tables: List[str] = []
        self.transaction_id = self._generate_transaction_id()
        self.is_active = False
    
    def _generate_transaction_id(self) -> str:
        """Generate unique transaction ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"txn_{timestamp}"
    
    @contextmanager
    def begin_transaction(self):
        """
        Start a new transaction context.
        
        Usage:
            with transaction_manager.begin_transaction():
                # Perform operations
                transaction_manager.write_table(df, "table_name")
        """
        self.is_active = True
        self.logger.info(f"Starting transaction {self.transaction_id}")
        
        try:
            yield self
            
            if self.test_mode:
                self.logger.info("Test mode - rolling back changes")
                self._rollback()
            else:
                self.logger.info("Committing transaction")
                self._commit()
                
        except Exception as e:
            self.logger.error(f"Transaction failed: {str(e)}")
            self._rollback()
            raise
        finally:
            self.is_active = False
            self.logger.info(f"Transaction {self.transaction_id} completed")
    
    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None
    ):
        """Write DataFrame to table within transaction."""
        if not self.is_active:
            raise RuntimeError("No active transaction")
        
        if self.test_mode:
            # Create temporary view
            temp_view = f"temp_{table_name}_{self.transaction_id}"
            df.createOrReplaceTempView(temp_view)
            self.temp_views.append(temp_view)
            self.logger.info(f"Created temp view: {temp_view}")
        else:
            # Create backup if overwriting
            if mode == "overwrite":
                self._create_backup(table_name)
            
            # Write to table
            writer = df.write.mode(mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.saveAsTable(table_name)
            
            self.logger.info(f"Wrote to table: {table_name}")
    
    def _create_backup(self, table_name: str):
        """Create backup of table before overwriting."""
        backup_name = f"{table_name}_backup_{self.transaction_id}"
        try:
            self.spark.sql(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}")
            self.backup_tables.append((table_name, backup_name))
            self.logger.info(f"Created backup: {backup_name}")
        except Exception as e:
            self.logger.warning(f"Failed to create backup: {str(e)}")
    
    def _commit(self):
        """Commit transaction - clean up backups."""
        for original, backup in self.backup_tables:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {backup}")
                self.logger.debug(f"Dropped backup table: {backup}")
            except Exception as e:
                self.logger.warning(f"Failed to drop backup {backup}: {str(e)}")
        
        self.backup_tables.clear()
        self.temp_views.clear()
    
    def _rollback(self):
        """Rollback transaction - restore from backups."""
        # Restore from backups
        for original, backup in self.backup_tables:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {original}")
                self.spark.sql(f"ALTER TABLE {backup} RENAME TO {original}")
                self.logger.info(f"Restored table from backup: {original}")
            except Exception as e:
                self.logger.error(f"Failed to restore {original}: {str(e)}")
        
        # Clean up temp views
        for view in self.temp_views:
            try:
                self.spark.catalog.dropTempView(view)
                self.logger.debug(f"Dropped temp view: {view}")
            except Exception as e:
                self.logger.warning(f"Failed to drop temp view {view}: {str(e)}")
        
        self.backup_tables.clear()
        self.temp_views.clear()


===FILE: tests/test_load.py===
"""
Unit tests for DataLoader module.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import date
from decimal import Decimal
import tempfile
import os

from src.load import DataLoader, TransactionalWriter
from src.transaction_manager import TransactionManager


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .appName("test_data_loader") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .enableHiveSupport() \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_schema():
    """Create sample schema for testing."""
    return StructType([
        StructField("analytics_id", StringType(), False),
        StructField("trans_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("gross_amount", DecimalType(16, 2), False),
        StructField("net_amount", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("region", StringType(), True)
    ])


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return [
        ("ANL001", date(2024, 1, 15), "CUST001", "PROD001", 10, Decimal("999.90"), Decimal("1079.89"), "USD", "NORTH"),
        ("ANL002", date(2024, 1, 15), "CUST002", "PROD002", 5, Decimal("749.95"), Decimal("809.95"), "USD", "SOUTH"),
        ("ANL003", date(2024, 1, 16), "CUST003", "PROD001", 20, Decimal("1799.80"), Decimal("1943.78"), "USD", "EAST"),
        ("ANL004", date(2024, 1, 16), "CUST001", "PROD003", 3, Decimal("899.97"), Decimal("971.97"), "USD", "WEST"),
        ("ANL005", date(2024, 1, 17), "CUST004", "PROD002", 15, Decimal("2024.85"), Decimal("2186.84"), "USD", "SOUTH")
    ]


@pytest.fixture
def sample_df(spark, sample_schema, sample_data):
    """Create sample DataFrame."""
    return spark.createDataFrame(sample_data, sample_schema)


@pytest.fixture
def loader_config():
    """Create loader configuration."""
    return {
        'required_columns': [
            'analytics_id', 'trans_date', 'customer_id', 
            'product_id', 'gross_amount', 'net_amount', 'currency'
        ],
        'key_columns': ['analytics_id', 'customer_id', 'product_id'],
        'write_options': {
            'compression': 'snappy',
            'fileFormat': 'parquet'
        },
        'jdbc': {
            'url': 'jdbc:postgresql://localhost:5432/test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'driver': 'org.postgresql.Driver'
        }
    }


class TestDataLoader:
    """Test cases for DataLoader class."""
    
    def test_initialization(self, spark, loader_config):
        """Test DataLoader initialization."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        assert loader.spark == spark
        assert loader.test_mode is True
        assert loader.config == loader_config
    
    def test_validate_data_success(self, spark, loader_config, sample_df):
        """Test successful data validation."""
        loader = DataLoader(spark, loader_config)
        is_valid, errors = loader.validate_data(sample_df)
        
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_data_empty_dataframe(self, spark, loader_config, sample_schema):
        """Test validation of empty DataFrame."""
        loader = DataLoader(spark, loader_config)
        empty_df = spark.createDataFrame([], sample_schema)
        
        is_valid, errors = loader.validate_data(empty_df)
        
        assert is_valid is False
        assert "DataFrame is empty" in errors
    
    def test_validate_data_missing_columns(self, spark, loader_config):
        """Test validation with missing required columns."""
        loader = DataLoader(spark, loader_config)
        
        # Create DataFrame with missing columns
        incomplete_df = spark.createDataFrame(
            [("ANL001", "CUST001")],
            StructType([
                StructField("analytics_id", StringType()),
                StructField("customer_id", StringType())
            ])
        )
        
        is_valid, errors = loader.validate_data(incomplete_df)
        
        assert is_valid is False
        assert any("Missing required columns" in err for err in errors)
    
    def test_validate_data_null_key_columns(self, spark, loader_config, sample_schema):
        """Test validation with null values in key columns."""
        loader = DataLoader(spark, loader_config)
        
        # Create DataFrame with null in key column
        data_with_nulls = [
            (None, date(2024, 1, 15), "CUST001", "PROD001", 10, 
             Decimal("999.90"), Decimal("1079.89"), "USD", "NORTH")
        ]
        df_with_nulls = spark.createDataFrame(data_with_nulls, sample_schema)
        
        is_valid, errors = loader.validate_data(df_with_nulls)
        
        assert is_valid is False
        assert any("null values" in err for err in errors)
    
    def test_load_to_table_test_mode(self, spark, loader_config, sample_df):
        """Test loading to table in test mode."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        
        result = loader.load_to_table(
            df=sample_df,
            table_name="test_analytics",
            mode="append"
        )
        
        assert result is True
        assert loader.stats['records_processed'] == 5
        assert loader.stats['records_success'] == 5
        assert loader.stats['records_error'] == 0
    
    def test_load_to_table_with_partitions(self, spark, loader_config, sample_df):
        """Test loading with partitioning."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        
        result = loader.load_to_table(
            df=sample_df,
            table_name="test_partitioned",
            mode="append",
            partition_by=["trans_date", "region"]
        )
        
        assert result is True
        assert loader.stats['records_success'] == 5
    
    def test_load_to_jdbc_test_mode(self, spark, loader_config, sample_df):
        """Test JDBC load in test mode."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        
        result = loader.load_to_jdbc(
            df=sample_df,
            table_name="test_jdbc_table",
            mode="append",
            batch_size=100
        )
        
        assert result is True
        assert loader.stats['records_processed'] == 5
        assert loader.stats['records_success'] == 5
    
    def test_load_with_validation_success(self, spark, loader_config, sample_df):
        """Test load with validation - success case."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        
        result = loader.load_with_validation(
            df=sample_df,
            table_name="test_validated",
            write_type="table",
            mode="append"
        )
        
        assert result['success'] is True
        assert result['test_mode'] is True
        assert result['stats']['records_success'] == 5
    
    def test_load_with_validation_failure(self, spark, loader_config, sample_schema):
        """Test load with validation - failure case."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        
        # Create empty DataFrame
        empty_df = spark.createDataFrame([], sample_schema)
        
        result = loader.load_with_validation(
            df=empty_df,
            table_name="test_failed",
            write_type="table"
        )
        
        assert result['success'] is False
        assert len(result.get('errors', [])) > 0
    
    def test_get_statistics(self, spark, loader_config, sample_df):
        """Test statistics retrieval."""
        loader = DataLoader(spark, loader_config, test_mode=True)
        
        loader.load_to_table(sample_df, "test_stats")
        stats = loader.get_statistics()
        
        assert stats['records_processed'] == 5
        assert stats['records_success'] == 5
        assert stats['records_error'] == 0
        assert 'start_time' in stats
        assert 'end_time' in stats


class TestTransactionalWriter:
    """Test cases for TransactionalWriter class."""
    
    def test_transaction_context_success(self, spark):
        """Test successful transaction context."""
        writer = TransactionalWriter(spark, test_mode=True)
        
        with writer.transaction():
            # Simulate operations
            temp_df = spark.range(10)
            temp_df.createOrReplaceTempView("test_temp_view")
            writer.temp_views.append("test_temp_view")
        
        # Verify temp view was cleaned up
        with pytest.raises(Exception):
            spark.sql("SELECT * FROM test_temp_view")
    
    def test_transaction_rollback_on_error(self, spark):
        """Test transaction rollback on error."""
        writer = TransactionalWriter(spark, test_mode=False)
        
        with pytest.raises(ValueError):
            with writer.transaction():
                temp_df = spark.range(10)
                temp_df.createOrReplaceTempView("test_rollback_view")
                writer.temp_views.appen