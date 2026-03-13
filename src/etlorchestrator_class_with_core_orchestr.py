===FILE: src/orchestrator.py===
"""
ETL Orchestrator Module
Main orchestrator that coordinates the complete ETL pipeline process
"""

from typing import Optional, Dict, Any
from datetime import datetime, date
from pyspark.sql import SparkSession
import uuid
import logging

from src.logger import ETLLogger
from src.extractor import ETLExtractor
from src.transformer import ETLTransformer
from src.loader import ETLLoader
from src.exceptions import ETLError


class ETLOrchestrator:
    """
    Main ETL orchestrator class that coordinates extraction, transformation,
    and loading of sales data through the complete pipeline.
    """

    def __init__(self, spark: Optional[SparkSession] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the ETL orchestrator with Spark session and configuration.

        Args:
            spark: SparkSession instance. If None, creates a new session.
            config: Configuration dictionary. If None, uses default config.
        """
        self.spark = spark or self._create_spark_session()
        self.config = config or {}
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            spark=self.spark,
            config=self.config
        )
        
        # Initialize ETL components
        self.extractor = ETLExtractor(
            spark=self.spark,
            logger=self.logger,
            config=self.config
        )
        
        self.transformer = ETLTransformer(
            spark=self.spark,
            logger=self.logger,
            config=self.config
        )
        
        self.loader = ETLLoader(
            spark=self.spark,
            logger=self.logger,
            config=self.config
        )
        
        # Execution tracking
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.execution_stats: Dict[str, Any] = {}
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )

    def _create_spark_session(self) -> SparkSession:
        """
        Create a new Spark session with optimized configurations.

        Returns:
            SparkSession: Configured Spark session
        """
        return (SparkSession.builder
                .appName("SalesETLOrchestrator")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate())

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.

        Returns:
            str: Unique ETL run ID with timestamp prefix
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_suffix = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}{unique_suffix}"

    def run_etl(
        self,
        from_date: date,
        to_date: date,
        test_mode: bool = False
    ) -> bool:
        """
        Execute the complete ETL pipeline process.

        This method orchestrates the three main phases:
        1. Extract: Pull raw sales data from source
        2. Transform: Apply business rules and calculations
        3. Load: Write analytics data to target

        Args:
            from_date: Start date for data extraction
            to_date: End date for data extraction
            test_mode: If True, run without committing data

        Returns:
            bool: True if ETL completed successfully, False otherwise

        Raises:
            ETLError: If critical error occurs during processing
        """
        success = False
        
        try:
            # Record start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            
            # ===================================================================
            # PHASE 1: EXTRACT
            # ===================================================================
            logging.info("=== EXTRACT Phase ===")
            self.logger.log_message(
                step='EXTRACT',
                status='I',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            raw_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_df is None or raw_df.count() == 0:
                raise ETLError(
                    error_text="No data extracted from source",
                    error_step="EXTRACT"
                )
            
            extract_count = raw_df.count()
            self.execution_stats['records_extracted'] = extract_count
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=extract_count,
                records_success=extract_count,
                message=f'Extracted {extract_count} records successfully'
            )
            
            # ===================================================================
            # PHASE 2: TRANSFORM
            # ===================================================================
            logging.info("=== TRANSFORM Phase ===")
            self.logger.log_message(
                step='TRANSFORM',
                status='I',
                message='Starting data transformation'
            )
            
            analytics_df = self.transformer.transform_data(
                raw_df=raw_df,
                etl_run_id=self.etl_run_id
            )
            
            if analytics_df is None or analytics_df.count() == 0:
                raise ETLError(
                    error_text="Transformation produced no records",
                    error_step="TRANSFORM"
                )
            
            transform_count = analytics_df.count()
            self.execution_stats['records_transformed'] = transform_count
            
            # Calculate transformation success rate
            transform_success_rate = (transform_count / extract_count) * 100 if extract_count > 0 else 0
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=extract_count,
                records_success=transform_count,
                records_error=extract_count - transform_count,
                message=f'Transformed {transform_count} of {extract_count} records ({transform_success_rate:.2f}%)'
            )
            
            # ===================================================================
            # PHASE 3: LOAD
            # ===================================================================
            logging.info("=== LOAD Phase ===")
            self.logger.log_message(
                step='LOAD',
                status='I',
                message='Starting data load to target'
            )
            
            load_result = self.loader.load_data(
                analytics_df=analytics_df,
                test_mode=test_mode
            )
            
            if not load_result.get('success', False):
                raise ETLError(
                    error_text="Load phase failed",
                    error_step="LOAD"
                )
            
            load_count = load_result.get('records_loaded', 0)
            self.execution_stats['records_loaded'] = load_count
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=transform_count,
                records_success=load_count,
                records_error=transform_count - load_count,
                message=f'Loaded {load_count} records successfully'
            )
            
            # ===================================================================
            # COMPLETION
            # ===================================================================
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.execution_stats['duration_seconds'] = duration
            
            completion_message = (
                f'ETL process completed successfully. '
                f'Extracted: {extract_count}, '
                f'Transformed: {transform_count}, '
                f'Loaded: {load_count}, '
                f'Duration: {duration:.2f}s'
            )
            
            if test_mode:
                completion_message += ' (TEST MODE - No data committed)'
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=completion_message
            )
            
            success = True
            logging.info(f"ETL completed successfully: {completion_message}")
            
        except ETLError as e:
            self.end_time = datetime.now()
            error_message = f'ETL process failed in {e.error_step}: {e.error_text}'
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=error_message
            )
            
            logging.error(error_message)
            success = False
            
        except Exception as e:
            self.end_time = datetime.now()
            error_message = f'Unexpected error in ETL process: {str(e)}'
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=error_message
            )
            
            logging.error(error_message, exc_info=True)
            success = False
            
        finally:
            # Ensure end time is set
            if self.end_time is None:
                self.end_time = datetime.now()
        
        return success

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.

        Returns:
            str: ETL run ID
        """
        return self.etl_run_id

    def display_summary(self) -> Dict[str, Any]:
        """
        Generate and display execution summary with statistics.

        Returns:
            dict: Dictionary containing execution summary and statistics
        """
        if self.start_time is None:
            return {
                'etl_run_id': self.etl_run_id,
                'status': 'NOT_STARTED',
                'message': 'ETL process has not been executed yet'
            }
        
        # Calculate duration
        duration_seconds = 0
        if self.end_time and self.start_time:
            duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        # Build summary dictionary
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration_seconds,
            'duration_formatted': self._format_duration(duration_seconds),
            'statistics': {
                'records_extracted': self.execution_stats.get('records_extracted', 0),
                'records_transformed': self.execution_stats.get('records_transformed', 0),
                'records_loaded': self.execution_stats.get('records_loaded', 0),
            }
        }
        
        # Calculate success rates
        extracted = summary['statistics']['records_extracted']
        if extracted > 0:
            summary['statistics']['transform_success_rate'] = (
                summary['statistics']['records_transformed'] / extracted * 100
            )
            summary['statistics']['load_success_rate'] = (
                summary['statistics']['records_loaded'] / extracted * 100
            )
        
        # Print formatted summary
        self._print_summary(summary)
        
        return summary

    def _format_duration(self, seconds: float) -> str:
        """
        Format duration in human-readable format.

        Args:
            seconds: Duration in seconds

        Returns:
            str: Formatted duration string
        """
        if seconds < 60:
            return f"{seconds:.2f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.2f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.2f} hours"

    def _print_summary(self, summary: Dict[str, Any]) -> None:
        """
        Print formatted execution summary to console.

        Args:
            summary: Summary dictionary to print
        """
        print("\n" + "=" * 70)
        print("ETL Process Execution Summary")
        print("=" * 70)
        print(f"ETL Run ID:        {summary['etl_run_id']}")
        print(f"Start Time:        {summary['start_time']}")
        print(f"End Time:          {summary['end_time']}")
        print(f"Duration:          {summary['duration_formatted']}")
        print("-" * 70)
        print("Statistics:")
        print(f"  Records Extracted:   {summary['statistics']['records_extracted']:,}")
        print(f"  Records Transformed: {summary['statistics']['records_transformed']:,}")
        print(f"  Records Loaded:      {summary['statistics']['records_loaded']:,}")
        
        if 'transform_success_rate' in summary['statistics']:
            print(f"  Transform Success:   {summary['statistics']['transform_success_rate']:.2f}%")
        if 'load_success_rate' in summary['statistics']:
            print(f"  Load Success:        {summary['statistics']['load_success_rate']:.2f}%")
        
        print("=" * 70 + "\n")

    def get_execution_stats(self) -> Dict[str, Any]:
        """
        Get detailed execution statistics.

        Returns:
            dict: Execution statistics dictionary
        """
        return self.execution_stats.copy()

    def cleanup(self) -> None:
        """
        Cleanup resources after ETL execution.
        """
        try:
            # Persist final logs
            if hasattr(self.logger, 'persist_logs'):
                self.logger.persist_logs()
            
            logging.info(f"ETL run {self.etl_run_id} cleanup completed")
            
        except Exception as e:
            logging.warning(f"Error during cleanup: {str(e)}")


===FILE: src/logger.py===
"""
ETL Logger Module
Provides logging functionality for ETL pipeline execution
"""

from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import uuid


class ETLLogger:
    """
    Logger class for ETL process execution tracking and audit trail.
    """

    def __init__(
        self,
        etl_run_id: str,
        spark: SparkSession,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL logger.

        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.config = config or {}
        self.log_entries = []

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
        Log a message with execution details.

        Args:
            step: ETL process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_timestamp': datetime.now(),
            'execution_date': datetime.now().date(),
            'execution_time': datetime.now().time(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        self.log_entries.append(log_entry)
        
        # Console output
        timestamp_str = log_entry['execution_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp_str}] [{step}] [{status}] {message}")

    def _generate_log_id(self) -> str:
        """
        Generate unique log entry identifier.

        Returns:
            str: Unique log ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"

    def get_etl_run_id(self) -> str:
        """
        Get the ETL run identifier.

        Returns:
            str: ETL run ID
        """
        return self.etl_run_id

    def get_log_entries(self) -> list:
        """
        Get all log entries for this ETL run.

        Returns:
            list: List of log entry dictionaries
        """
        return self.log_entries.copy()

    def persist_logs(self, target_path: Optional[str] = None) -> None:
        """
        Persist log entries to storage.

        Args:
            target_path: Optional path to write logs. Uses config if not provided.
        """
        if not self.log_entries:
            return
        
        target_path = target_path or self.config.get('log_table_path', 'etl_logs')
        
        # Create DataFrame from log entries
        log_df = self.spark.createDataFrame(self.log_entries)
        
        # Write to target
        log_df.write.mode('append').parquet(target_path)


===FILE: src/extractor.py===
"""
ETL Extractor Module
Handles data extraction from source systems
"""

from typing import Optional
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, lit

from src.logger import ETLLogger


class ETLExtractor:
    """
    Extractor component for reading raw sales data from source.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config

    def extract_data(
        self,
        from_date: date,
        to_date: date
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data from source.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction

        Returns:
            DataFrame: Raw sales data or None if extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='I',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Define schema
            schema = self._get_raw_schema()
            
            # Get source path from config or use default
            source_path = self.config.get('source_table_path', 'raw_sales')
            
            # Read from source
            # In production, this would read from actual source table/files
            df = self._read_from_source(source_path, schema, from_date, to_date)
            
            count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=count,
                records_success=count,
                message=f'Extracted {count} records successfully'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            return None

    def _get_raw_schema(self) -> StructType:
        """
        Get schema definition for raw sales data.

        Returns:
            StructType: Schema definition
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

    def _read_from_source(
        self,
        source_path: str,
        schema: StructType,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Read data from source with date filtering.

        Args:
            source_path: Source data path
            schema: Schema definition
            from_date: Start date
            to_date: End date

        Returns:
            DataFrame: Filtered source data
        """
        # For demonstration, create sample data
        # In production, use: spark.read.parquet(source_path) or similar
        sample_data = [
            ('T000001', from_date, 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', from_date, 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', from_date, 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', from_date, 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', from_date, 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema)
        
        # Apply date filter
        df = df.filter(
            (col("trans_date") >= lit(from_date)) &
            (col("trans_date") <= lit(to_date)) &
            (col("status") == lit('N'))
        )
        
        return df


===FILE: src/transformer.py===
"""
ETL Transformer Module
Applies business logic and transformations to raw data
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType

from src.logger import ETLLogger


class ETLTransformer:
    """
    Transformer component for applying business rules to sales data.
    """

    def __init__(self, spark, logger: ETLLogger, config: dict):
        """
        Initialize transformer.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Business rule constants from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)

    def transform_data(
        self,
        raw_df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier

        Returns:
            DataFrame: Transformed analytics data
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='I',
                message='Starting data transformation'
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            analytics_df = (
                raw_df
                .withColumn('gross_amount', col('quantity') * col('unit_price'))
                .withColumn('discount_amount', self._calculate_discount())
                .withColumn('tax_amount', 
                    (col('gross_amount') - col('discount_amount')) * lit(self.tax_rate)
                )
                .withColumn('net_amount',
                    col('gross_amount') - col('discount_amount') + col('tax_amount')
                )
                .withColumn('cost_amount',
                    col('quantity') * col('unit_price') * lit(self.cost_ratio)
                )
                .withColumn('profit_margin',
                    when(col('net_amount') > 0,
                        ((col('net_amount') - col('cost_amount')) / col('net_amount')) * lit(100)
                    ).otherwise(lit(0))
                )
                .withColumn('category', self._categorize_sale())
                .withColumn('analytics_id', self._generate_analytics_id())
                .withColumn('etl_run_id', lit(etl_run_id))
                .withColumn('loaded_at', current_timestamp())
                .select(
                    'analytics_id',
                    'trans_date',
                    'customer_id',
                    'product_id',
                    col('quantity').alias('total_quantity'),
                    spark_round('gross_amount', 2).alias('gross_amount'),
                    spark_round('net_amount', 2).alias('net_amount'),
                    spark_round('discount_amount', 2).alias('discount_amount'),
                    spark_round('tax_amount', 2).alias('tax_amount'),
                    'currency',
                    'sales_rep',
                    'region',
                    spark_round('profit_margin', 2).alias('profit_margin'),
                    'category',
                    'etl_run_id',
                    'loaded_at'
                )
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
            raise

    def _calculate_discount(self):
        """
        Calculate discount amount based on quantity tiers.

        Returns:
            Column: Discount calculation expression
        """
        return when(
            col('quantity') > self.discount_qty_tier2,
            col('gross_amount') * lit(self.discount_rate_tier2)
        ).when(
            col('quantity') > self.discount_qty_tier1,
            col('gross_amount') * lit(self.discount_rate_tier1)
        ).otherwise(lit(0))

    def _categorize_sale(self):
        """
        Categorize sales as HIGH, MEDIUM, or LOW based on gross amount.

        Returns:
            Column: Category assignment expression
        """
        return when(
            col('gross_amount') >= self.category_high_threshold,
            lit('HIGH')
        ).when(
            col('gross_amount') >= self.category_medium_threshold,
            lit('MEDIUM')
        ).otherwise(lit('LOW'))

    def _generate_analytics_id(self):
        """
        Generate unique analytics ID.

        Returns:
            Column: Analytics ID generation expression
        """
        from pyspark.sql.functions import concat, lit, date_format, monotonically_increasing_id
        
        return concat(
            lit('ANL'),
            col('trans_id'),
            date_format(current_timestamp(), 'yyyyMMddHHmmss'),
            lit('_'),
            monotonically_increasing_id()
        )


===FILE: src/loader.py===
"""
ETL Loader Module
Handles loading of transformed data to target systems
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.logger import ETLLogger


class ETLLoader:
    """
    Loader component for writing analytics data to target.
    """

    def __init__(self, spark, logger: ETLLogger, config: dict):
        """
        Initialize loader.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config

    def load_data(
        self,
        analytics_df: DataFrame,
        test_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Load analytics data to target table.

        Args:
            analytics_df: Transformed analytics DataFrame
            test_mode: If True, validate but don't write

        Returns:
            dict: Load result with success status and counts
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='I',
                message='Starting data load to target'
            )
            
            # Validate records before loading
            validated_df = self._validate_records(analytics_df)
            
            input_count = analytics_df.count()
            valid_count = validated_df.count()
            invalid_count = input_count - valid_count
            
            if invalid_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message=f'{invalid_count} invalid records filtered out'
                )
            
            # Load to target
            if not test_mode and valid_count > 0:
                target_path = self.config.get('target_table_path', 'analytics_sales')
                
                validated_df.write \
                    .mode('append') \
                    .partitionBy('trans_date') \
                    .parquet(target_path)
                
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    records_processed=input_count,
                    records_success=valid_count,
                    records_error=invalid_count,
                    message=f'Loaded {valid_count} records successfully'
                )
            else:
                mode_msg = "(TEST MODE)" if test_mode else ""
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Load validation complete {mode_msg}: {valid_count} valid records'
                )
            
            return {
                'success': True,
                'records_loaded': valid_count,
                'records_invalid': invalid_count,
                'test