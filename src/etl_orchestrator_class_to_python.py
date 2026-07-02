===FILE: src/orchestrator.py===
"""
ETL Orchestrator Module
Main orchestrator that coordinates the ETL process for sales data.
Migrated from ABAP class ZCL_ETL_ORCHESTRATOR.
"""

from datetime import datetime
from typing import Tuple
from pyspark.sql import SparkSession

from src.logger import ETLLogger
from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the extract, transform, and load processes.
    
    Attributes:
        logger: ETL logging instance
        extractor: Data extraction component
        transformer: Data transformation component
        loader: Data loading component
        etl_run_id: Unique identifier for this ETL run
        start_time: Process start timestamp
        end_time: Process end timestamp
    """
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the ETL orchestrator with all required components.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary with ETL parameters
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            spark=spark,
            etl_run_id=self.etl_run_id,
            config=config
        )
        
        # Initialize ETL components
        self.extractor = ETLExtractor(
            spark=spark,
            logger=self.logger,
            config=config
        )
        
        self.transformer = ETLTransformer(
            spark=spark,
            logger=self.logger,
            config=config
        )
        
        self.loader = ETLLoader(
            spark=spark,
            logger=self.logger,
            config=config
        )
        
        # Initialize timing attributes
        self.start_time = None
        self.end_time = None
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
    
    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL process: Extract, Transform, Load.
        
        Args:
            from_date: Start date for data extraction (YYYY-MM-DD)
            to_date: End date for data extraction (YYYY-MM-DD)
            
        Returns:
            bool: True if ETL completed successfully, False otherwise
        """
        extract_success = False
        transform_success = False
        load_success = False
        
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            
            # Step 1: Extract
            print("=" * 60)
            print("=== EXTRACT Phase ===")
            print("=" * 60)
            
            raw_data_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data_df is None or raw_data_df.count() == 0:
                raise ValueError("Extraction failed: No data retrieved")
            
            extract_success = True
            
            # Step 2: Transform
            print("\n" + "=" * 60)
            print("=== TRANSFORM Phase ===")
            print("=" * 60)
            
            analytics_df = self.transformer.transform_data(raw_data_df)
            
            if analytics_df is None or analytics_df.count() == 0:
                raise ValueError("Transformation failed: No data transformed")
            
            transform_success = True
            
            # Step 3: Load
            print("\n" + "=" * 60)
            print("=== LOAD Phase ===")
            print("=" * 60)
            
            load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise ValueError("Load failed: Unable to persist data")
            
            # Capture end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time.isoformat()}'
            )
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            return False
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate a unique ETL run identifier.
        
        Returns:
            str: Unique ETL run ID in format 'ETL_YYYYMMDDHHMMSS'
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f'ETL_{timestamp}'
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: Current ETL run ID
        """
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """
        Display a summary of the ETL execution including timing and statistics.
        """
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time.isoformat() if self.start_time else 'N/A'}")
        print(f"End Time:      {self.end_time.isoformat() if self.end_time else 'N/A'}")
        
        # Calculate duration
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("=" * 60)


===FILE: src/extract.py===
"""
ETL Extractor Module
Extracts raw sales data from source tables/files.
Migrated from ABAP class ZCL_ETL_EXTRACTOR.
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger


class ETLExtractor:
    """
    Data extraction component for the ETL process.
    Responsible for reading raw sales data from source systems.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the extractor component.
        
        Args:
            spark: Active SparkSession
            logger: Logger instance for tracking extraction progress
            config: Configuration dictionary with extraction parameters
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.raw_sales_schema = self._define_raw_sales_schema()
    
    def _define_raw_sales_schema(self) -> StructType:
        """
        Define the schema for raw sales data.
        
        Returns:
            StructType: Schema definition for raw sales records
        """
        return StructType([
            StructField("trans_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), True),
            StructField("region", StringType(), True),
            StructField("status", StringType(), False)
        ])
    
    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'
            
        Returns:
            DataFrame: Raw sales data or None if extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Read from source (adapt based on actual source: JDBC, Parquet, CSV, etc.)
            source_path = self.config.get('source_path', 'data/raw/sales')
            
            # For demonstration, create sample data
            # In production, replace with actual data source read
            if self.config.get('use_sample_data', False):
                df = self._create_sample_data()
            else:
                # Example: Read from Parquet
                df = self.spark.read.schema(self.raw_sales_schema).parquet(source_path)
                
                # Filter by date range and status
                df = df.filter(
                    (df.trans_date >= from_date) &
                    (df.trans_date <= to_date) &
                    (df.status == 'N')
                )
            
            record_count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            return None
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample raw sales data for testing purposes.
        
        Returns:
            DataFrame: Sample sales data
        """
        from datetime import date
        
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N')
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.raw_sales_schema)


===FILE: src/transform.py===
"""
ETL Transformer Module
Transforms raw sales data into analytics format with business logic.
Migrated from ABAP class ZCL_ETL_TRANSFORMER.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger


class ETLTransformer:
    """
    Data transformation component for the ETL process.
    Applies business rules and calculations to raw sales data.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the transformer component.
        
        Args:
            spark: Active SparkSession
            logger: Logger instance for tracking transformation progress
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.analytics_schema = self._define_analytics_schema()
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)
    
    def _define_analytics_schema(self) -> StructType:
        """
        Define the schema for analytics data.
        
        Returns:
            StructType: Schema definition for analytics records
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
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_data_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_data_df: DataFrame containing raw sales records
            
        Returns:
            DataFrame: Transformed analytics data or None if transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            input_count = raw_data_df.count()
            
            # Calculate gross amount
            df = raw_data_df.withColumn(
                'gross_amount',
                F.col('quantity') * F.col('unit_price')
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                'discount_amount',
                F.when(F.col('quantity') > self.discount_qty_tier2, 
                       F.col('gross_amount') * F.lit(self.discount_rate_tier2))
                .when(F.col('quantity') > self.discount_qty_tier1,
                      F.col('gross_amount') * F.lit(self.discount_rate_tier1))
                .otherwise(F.lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            df = df.withColumn(
                'tax_amount',
                (F.col('gross_amount') - F.col('discount_amount')) * F.lit(self.tax_rate)
            )
            
            # Calculate net amount
            df = df.withColumn(
                'net_amount',
                F.col('gross_amount') - F.col('discount_amount') + F.col('tax_amount')
            )
            
            # Calculate cost and profit margin
            df = df.withColumn('cost', F.col('quantity') * F.col('unit_price') * F.lit(self.cost_ratio))
            df = df.withColumn(
                'profit_margin',
                F.when(F.col('net_amount') > 0,
                       ((F.col('net_amount') - F.col('cost')) / F.col('net_amount')) * 100)
                .otherwise(F.lit(0.0))
            )
            
            # Categorize sales
            df = df.withColumn(
                'category',
                F.when(F.col('gross_amount') >= self.category_high_threshold, F.lit('HIGH'))
                .when(F.col('gross_amount') >= self.category_medium_threshold, F.lit('MEDIUM'))
                .otherwise(F.lit('LOW'))
            )
            
            # Generate analytics ID
            df = df.withColumn(
                'analytics_id',
                F.concat(
                    F.lit('ANL_'),
                    F.col('trans_id'),
                    F.lit('_'),
                    F.date_format(F.current_timestamp(), 'yyyyMMddHHmmss')
                )
            )
            
            # Add ETL run ID
            df = df.withColumn('etl_run_id', F.lit(self.logger.etl_run_id))
            
            # Select and rename columns to match analytics schema
            analytics_df = df.select(
                'analytics_id',
                'trans_date',
                'customer_id',
                'product_id',
                F.col('quantity').alias('total_quantity'),
                'gross_amount',
                'net_amount',
                'discount_amount',
                'tax_amount',
                'currency',
                'sales_rep',
                'region',
                'profit_margin',
                'category',
                'etl_run_id'
            )
            
            output_count = analytics_df.count()
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count,
                message=f'Transformed {output_count} of {input_count} records'
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            return None


===FILE: src/load.py===
"""
ETL Loader Module
Loads transformed analytics data into target storage.
Migrated from ABAP class ZCL_ETL_LOADER.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.logger import ETLLogger


class ETLLoader:
    """
    Data loading component for the ETL process.
    Responsible for persisting transformed analytics data to target storage.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the loader component.
        
        Args:
            spark: Active SparkSession
            logger: Logger instance for tracking load progress
            config: Configuration dictionary with load parameters
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load transformed analytics data to target storage.
        
        Args:
            analytics_df: DataFrame containing transformed analytics records
            
        Returns:
            bool: True if load succeeded, False otherwise
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            total_count = analytics_df.count()
            
            # Validate records before loading
            valid_df = analytics_df.filter(
                (F.col('analytics_id').isNotNull()) &
                (F.col('customer_id').isNotNull()) &
                (F.col('product_id').isNotNull()) &
                (F.col('gross_amount') > 0) &
                (F.col('currency').isNotNull()) &
                (F.col('category').isin(['HIGH', 'MEDIUM', 'LOW']))
            )
            
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message=f'{error_count} invalid records filtered out'
                )
            
            # Write to target storage
            target_path = self.config.get('target_path', 'data/analytics/sales')
            write_mode = self.config.get('write_mode', 'append')
            
            valid_df.write.mode(write_mode).parquet(target_path)
            
            # In production, you might also update source table status
            # Example: UPDATE source SET status = 'P' WHERE trans_id IN (...)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
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


===FILE: src/logger.py===
"""
ETL Logger Module
Provides logging functionality for ETL processes.
Migrated from ABAP class ZCL_ETL_LOGGER.
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class ETLLogger:
    """
    Logging utility for ETL processes.
    Tracks execution progress, statistics, and errors.
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: dict):
        """
        Initialize the logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique identifier for this ETL run
            config: Configuration dictionary with logging parameters
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_entries = []
        self.log_schema = self._define_log_schema()
    
    def _define_log_schema(self) -> StructType:
        """
        Define the schema for log entries.
        
        Returns:
            StructType: Schema definition for log records
        """
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True)
        ])
    
    def log_message(
        self,
        step: str,
        status: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        message: str = ''
    ) -> None:
        """
        Log a message for the ETL process.
        
        Args:
            step: Process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            records_processed: Total number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
            message: Descriptive message
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_timestamp': datetime.now(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        self.log_entries.append(log_entry)
        
        # Console output
        timestamp_str = log_entry['execution_timestamp'].strftime('%H:%M:%S')
        print(f"{timestamp_str} [{step:12s}] [{status}] {message}")
        
        # Optionally persist to database or file
        if self.config.get('persist_logs', False):
            self._persist_log_entry(log_entry)
    
    def _generate_log_id(self) -> str:
        """
        Generate a unique log entry identifier.
        
        Returns:
            str: Unique log ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f'LOG_{timestamp}'
    
    def _persist_log_entry(self, log_entry: dict) -> None:
        """
        Persist log entry to storage.
        
        Args:
            log_entry: Dictionary containing log information
        """
        try:
            log_path = self.config.get('log_path', 'data/logs/etl')
            log_df = self.spark.createDataFrame([log_entry], schema=self.log_schema)
            log_df.write.mode('append').parquet(log_path)
        except Exception as e:
            print(f"Warning: Failed to persist log entry: {str(e)}")
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: ETL run ID
        """
        return self.etl_run_id


===FILE: src/constants.py===
"""
ETL Constants Module
Defines constants and configuration values for the ETL system.
Migrated from ABAP class ZCL_ETL_CONSTANTS.
"""


class ETLConstants:
    """
    Constants and configuration values for the ETL system.
    """
    
    # Status codes
    class Status:
        NEW = 'N'
        PROCESSED = 'P'
        ERROR = 'E'
        WARNING = 'W'
        SUCCESS = 'S'
        INFO = 'I'
    
    # ETL process steps
    class Step:
        INIT = 'INIT'
        EXTRACT = 'EXTRACT'
        TRANSFORM = 'TRANSFORM'
        LOAD = 'LOAD'
        VALIDATE = 'VALIDATE'
        COMPLETE = 'COMPLETE'
        ERROR = 'ERROR'
    
    # Sale categories
    class Category:
        HIGH = 'HIGH'
        MEDIUM = 'MEDIUM'
        LOW = 'LOW'
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = 0.05
    DISCOUNT_RATE_TIER2 = 0.10
    
    # Business rules - Tax rate
    TAX_RATE = 0.08
    
    # Business rules - Cost ratio
    COST_RATIO = 0.60
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = 2000.00
    CATEGORY_MEDIUM_THRESHOLD = 500.00
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN = 'ETL'
    PREFIX_LOG_ID = 'LOG'
    PREFIX_ANALYTICS_ID = 'ANL'
    
    # Message texts
    MSG_INIT_SUCCESS = 'ETL process initialized successfully'
    MSG_EXTRACT_START = 'Starting data extraction'
    MSG_EXTRACT_COMPLETE = 'Data extraction completed'
    MSG_TRANSFORM_START = 'Starting data transformation'
    MSG_TRANSFORM_COMPLETE = 'Data transformation completed'
    MSG_LOAD_START = 'Starting data load'
    MSG_LOAD_COMPLETE = 'Data load completed'
    MSG_ETL_COMPLETE = 'ETL process completed successfully'
    MSG_ETL_ERROR = 'ETL process failed'


===FILE: config.yaml===
# ETL Configuration File
# Configuration parameters for Sales ETL System

# Application settings
application:
  name: "Sales ETL System"
  version: "1.0.0"
  environment: "development"

# Spark configuration
spark:
  app_name: "SalesETL"
  master: "local[*]"
  log_level: "WARN"
  config:
    spark.sql.shuffle.partitions: "10"
    spark.sql.adaptive.enabled: "true"

# Data source configuration
source:
  path: "data/raw/sales"
  format: "parquet"
  use_sample_data: true  # Set to false in production

# Data target configuration
target:
  path: "data/analytics/sales"
  format: "parquet"
  write_mode: "append"  # Options: append, overwrite

# Business rules - Discount configuration
discount_qty_tier1: 10
discount_qty_tier2: 15
discount_rate_tier1: 0.05
discount_rate_tier2: 0.10

# Business rules - Tax and cost
tax_rate: 0.08
cost_ratio: 0.60

# Business rules - Category thresholds
category_high_threshold: 2000.00
category_medium_threshold: 500.00

# ETL processing configuration
etl:
  batch_size: 1000
  commit_interval: 500
  retry_attempts: 3
  timeout_seconds: 3600
  parallel_jobs: 4

# Logging configuration
logging:
  persist_logs: true
  log_path: "data/logs/etl"
  console_log_level: "INFO"
  file_log_level: "DEBUG"

# Date range defaults (days back from current date)
date_defaults:
  from_date_offset: -7  # 7 days ago
  to_date_offset: 0     # today

# Testing configuration
testing:
  test_mode: true
  sample_record_count: 5


===FILE: main.py===
"""
Main ETL Application Entry Point
Executable program for running the Sales ETL process.
Migrated from ABAP report Z_SALES_ETL_MAIN.
"""

import argparse
import sys
from datetime import date, timedelta
import yaml
from pyspark.sql import SparkSession

from src.orchestrator import ETLOrchestrator


def load_config(config_path: str = 'config.yaml') -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        dict: Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)