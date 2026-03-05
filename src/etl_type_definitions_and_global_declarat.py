===FILE: src/extract.py===
"""
Data extraction module for Sales ETL system.
Extracts raw sales data from source tables.
"""
from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from datetime import date
import logging

from src.logger import ETLLogger


class SalesExtractor:
    """Handles extraction of raw sales data from source systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self._schema = self._get_raw_sales_schema()
    
    @staticmethod
    def _get_raw_sales_schema() -> StructType:
        """Define schema for raw sales data."""
        return StructType([
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
        ])
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: str = None,
        source_table: str = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for given date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Optional file path for source data
            source_table: Optional table name for database source
            
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Extract from source
            if source_table:
                df = self._extract_from_table(source_table, from_date, to_date)
            elif source_path:
                df = self._extract_from_file(source_path, from_date, to_date)
            else:
                # Generate sample data for demonstration
                df = self._generate_sample_data()
            
            # Filter by date range
            df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
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
            logging.error(f"Extraction error: {str(e)}", exc_info=True)
            return self.spark.createDataFrame([], self._schema), False
    
    def _extract_from_table(
        self,
        table_name: str,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """Extract data from database table."""
        query = f"""
            SELECT 
                trans_id,
                trans_date,
                customer_id,
                product_id,
                quantity,
                unit_price,
                currency,
                sales_rep,
                region,
                status
            FROM {table_name}
            WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
                AND status = 'N'
        """
        return self.spark.sql(query)
    
    def _extract_from_file(
        self,
        file_path: str,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """Extract data from file (CSV, Parquet, etc.)."""
        # Detect file format and read accordingly
        if file_path.endswith('.parquet'):
            df = self.spark.read.parquet(file_path)
        elif file_path.endswith('.csv'):
            df = self.spark.read.csv(file_path, header=True, schema=self._schema)
        else:
            df = self.spark.read.format("delta").load(file_path)
        
        return df
    
    def _generate_sample_data(self) -> DataFrame:
        """Generate sample sales data for demonstration."""
        from datetime import datetime
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self._schema)


===FILE: src/transform.py===
"""
Data transformation module for Sales ETL system.
Transforms raw sales data into analytics format with business rules applied.
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, 
    date_format, expr, round as spark_round
)
from decimal import Decimal
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesTransformer:
    """Handles transformation of raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self._schema = self._get_analytics_schema()
    
    @staticmethod
    def _get_analytics_schema() -> StructType:
        """Define schema for analytics data."""
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=False),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=False),
            StructField("discount_amount", DecimalType(16, 2), nullable=False),
            StructField("tax_amount", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            Tuple of (transformed DataFrame, success flag)
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._calculate_analytics(raw_df)
            
            # Validate transformed data
            transformed_df = transformed_df.filter(
                col("gross_amount").isNotNull() &
                (col("gross_amount") > 0)
            )
            
            success_count = transformed_df.count()
            error_count = initial_count - success_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Transformed {success_count} of {initial_count} records"
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            logging.error(f"Transformation error: {str(e)}", exc_info=True)
            return self.spark.createDataFrame([], self._schema), False
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """Apply business logic and calculate analytics fields."""
        # Get business rule thresholds from config
        discount_tier1_qty = self.config.get_discount_tier1_qty()
        discount_tier2_qty = self.config.get_discount_tier2_qty()
        discount_rate1 = self.config.get_discount_rate_tier1()
        discount_rate2 = self.config.get_discount_rate_tier2()
        tax_rate = self.config.get_tax_rate()
        cost_ratio = self.config.get_cost_ratio()
        high_threshold = self.config.get_category_high_threshold()
        medium_threshold = self.config.get_category_medium_threshold()
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_tier2_qty, 
                 spark_round(col("gross_amount") * discount_rate2, 2))
            .when(col("quantity") > discount_tier1_qty,
                  spark_round(col("gross_amount") * discount_rate1, 2))
            .otherwise(lit(0.00))
        )
        
        # Calculate tax on (gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * tax_rate, 2)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            spark_round(
                col("gross_amount") - col("discount_amount") + col("tax_amount"),
                2
            )
        )
        
        # Calculate profit margin
        # Cost = quantity * unit_price * cost_ratio
        df = df.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * cost_ratio, 2)
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                     2
                 ))
            .otherwise(lit(0.00))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= high_threshold, lit("HIGH"))
            .when(col("gross_amount") >= medium_threshold, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL run ID
        df = df.withColumn(
            "etl_run_id",
            lit(self.logger.get_etl_run_id())
        )
        
        # Add total quantity (rename from quantity)
        df = df.withColumn("total_quantity", col("quantity"))
        
        # Select final columns
        return df.select(
            "analytics_id",
            "trans_date",
            "customer_id",
            "product_id",
            "total_quantity",
            "gross_amount",
            "net_amount",
            "discount_amount",
            "tax_amount",
            "currency",
            "sales_rep",
            "region",
            "profit_margin",
            "category",
            "etl_run_id"
        )


===FILE: src/load.py===
"""
Data loading module for Sales ETL system.
Loads transformed analytics data into target tables/storage.
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesLoader:
    """Handles loading of analytics data into target systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: str = None,
        target_table: str = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target destination.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional file path for target storage
            target_table: Optional table name for database target
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Success flag
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            total_count = analytics_df.count()
            
            # Validate records before loading
            valid_df = self._validate_records(analytics_df)
            success_count = valid_df.count()
            error_count = total_count - success_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Load to target
            if target_table:
                self._load_to_table(valid_df, target_table, mode)
            elif target_path:
                self._load_to_file(valid_df, target_path, mode)
            else:
                # For demonstration, show sample output
                self._display_sample(valid_df)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Loaded {success_count} of {total_count} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            logging.error(f"Load error: {str(e)}", exc_info=True)
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """Validate records before loading."""
        # Filter out invalid records
        valid_df = df.filter(
            col("analytics_id").isNotNull() &
            (col("analytics_id") != "") &
            col("customer_id").isNotNull() &
            (col("customer_id") != "") &
            col("product_id").isNotNull() &
            (col("product_id") != "") &
            (col("gross_amount") > 0) &
            col("currency").isNotNull() &
            (col("currency") != "") &
            col("category").isin("HIGH", "MEDIUM", "LOW")
        )
        
        return valid_df
    
    def _load_to_table(self, df: DataFrame, table_name: str, mode: str):
        """Load data to database table."""
        df.write.mode(mode).saveAsTable(table_name)
        logging.info(f"Data loaded to table: {table_name}")
    
    def _load_to_file(self, df: DataFrame, file_path: str, mode: str):
        """Load data to file storage."""
        # Detect format and write accordingly
        if file_path.endswith('.parquet'):
            df.write.mode(mode).parquet(file_path)
        elif file_path.endswith('.csv'):
            df.write.mode(mode).csv(file_path, header=True)
        else:
            # Default to Delta format
            df.write.mode(mode).format("delta").save(file_path)
        
        logging.info(f"Data loaded to path: {file_path}")
    
    def _display_sample(self, df: DataFrame):
        """Display sample output for demonstration."""
        logging.info("Sample analytics data:")
        df.show(10, truncate=False)
    
    def update_source_status(
        self,
        processed_ids: list,
        source_table: str = None
    ) -> bool:
        """
        Update status of processed records in source table.
        
        Args:
            processed_ids: List of transaction IDs that were processed
            source_table: Source table name
            
        Returns:
            Success flag
        """
        try:
            if not source_table:
                return True
            
            # In production, execute UPDATE statement
            # For now, just log the operation
            self.logger.log_message(
                step="LOAD",
                status="S",
                message=f"Updated {len(processed_ids)} source records to processed status"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Failed to update source status: {str(e)}"
            )
            return False


===FILE: src/logger.py===
"""
Logging utility for ETL system.
Provides structured logging with ETL-specific metadata.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import logging


@dataclass
class LogEntry:
    """Represents a single ETL log entry."""
    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    
    def __str__(self) -> str:
        return (
            f"[{self.execution_time}] {self.process_step} | "
            f"{self.status} | {self.message} | "
            f"Processed: {self.records_processed}, "
            f"Success: {self.records_success}, "
            f"Errors: {self.records_error}"
        )


class ETLLogger:
    """Centralized logging for ETL processes."""
    
    def __init__(self, etl_run_id: str):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure Python logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(f"ETL-{self.etl_run_id}")
    
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
        Log an ETL message.
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime("%Y-%m-%d"),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self.log_entries.append(log_entry)
        
        # Log to console/file based on status
        log_msg = str(log_entry)
        if status == 'E':
            self.logger.error(log_msg)
        elif status == 'W':
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries for this run."""
        return self.log_entries
    
    def display_summary(self):
        """Display summary of all log entries."""
        print("\n" + "=" * 80)
        print("ETL Execution Summary")
        print("=" * 80)
        print(f"ETL Run ID: {self.etl_run_id}")
        print(f"Total Log Entries: {len(self.log_entries)}")
        print("-" * 80)
        
        for entry in self.log_entries:
            print(entry)
        
        print("=" * 80 + "\n")


===FILE: src/config.py===
"""
Configuration management for ETL system.
Loads and provides access to ETL configuration parameters.
"""
from dataclasses import dataclass
from decimal import Decimal
import yaml
import os
from typing import Optional


@dataclass
class StatusCodes:
    """Status code constants."""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass
class ProcessSteps:
    """ETL process step constants."""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass
class Categories:
    """Sale category constants."""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'


class ETLConfig:
    """ETL configuration manager."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.status = StatusCodes()
        self.steps = ProcessSteps()
        self.categories = Categories()
    
    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            # Return default configuration
            return self._get_default_config()
    
    def _get_default_config(self) -> dict:
        """Get default configuration values."""
        return {
            'business_rules': {
                'discount': {
                    'tier1_quantity': 10,
                    'tier2_quantity': 15,
                    'tier1_rate': 0.05,
                    'tier2_rate': 0.10
                },
                'tax_rate': 0.08,
                'cost_ratio': 0.60,
                'category_thresholds': {
                    'high': 2000.00,
                    'medium': 500.00
                }
            },
            'etl_settings': {
                'batch_size': 1000,
                'commit_interval': 500,
                'retry_attempts': 3,
                'timeout_seconds': 3600,
                'parallel_jobs': 4
            },
            'id_prefixes': {
                'etl_run': 'ETL',
                'log': 'LOG',
                'analytics': 'ANL'
            },
            'messages': {
                'init_success': 'ETL process initialized successfully',
                'extract_start': 'Starting data extraction',
                'extract_complete': 'Data extraction completed',
                'transform_start': 'Starting data transformation',
                'transform_complete': 'Data transformation completed',
                'load_start': 'Starting data load',
                'load_complete': 'Data load completed',
                'etl_complete': 'ETL process completed successfully',
                'etl_error': 'ETL process failed'
            }
        }
    
    # Business Rules Accessors
    def get_discount_tier1_qty(self) -> int:
        return self.config['business_rules']['discount']['tier1_quantity']
    
    def get_discount_tier2_qty(self) -> int:
        return self.config['business_rules']['discount']['tier2_quantity']
    
    def get_discount_rate_tier1(self) -> Decimal:
        return Decimal(str(self.config['business_rules']['discount']['tier1_rate']))
    
    def get_discount_rate_tier2(self) -> Decimal:
        return Decimal(str(self.config['business_rules']['discount']['tier2_rate']))
    
    def get_tax_rate(self) -> Decimal:
        return Decimal(str(self.config['business_rules']['tax_rate']))
    
    def get_cost_ratio(self) -> Decimal:
        return Decimal(str(self.config['business_rules']['cost_ratio']))
    
    def get_category_high_threshold(self) -> Decimal:
        return Decimal(str(self.config['business_rules']['category_thresholds']['high']))
    
    def get_category_medium_threshold(self) -> Decimal:
        return Decimal(str(self.config['business_rules']['category_thresholds']['medium']))
    
    # ETL Settings Accessors
    def get_batch_size(self) -> int:
        return self.config['etl_settings']['batch_size']
    
    def get_commit_interval(self) -> int:
        return self.config['etl_settings']['commit_interval']
    
    def get_retry_attempts(self) -> int:
        return self.config['etl_settings']['retry_attempts']
    
    def get_timeout_seconds(self) -> int:
        return self.config['etl_settings']['timeout_seconds']
    
    def get_parallel_jobs(self) -> int:
        return self.config['etl_settings']['parallel_jobs']
    
    # Message Accessors
    def get_message(self, key: str) -> str:
        return self.config['messages'].get(key, '')
    
    # ID Prefix Accessors
    def get_etl_prefix(self) -> str:
        return self.config['id_prefixes']['etl_run']
    
    def get_log_prefix(self) -> str:
        return self.config['id_prefixes']['log']
    
    def get_analytics_prefix(self) -> str:
        return self.config['id_prefixes']['analytics']


===FILE: src/orchestrator.py===
"""
ETL orchestration module.
Coordinates the execution of extract, transform, and load phases.
"""
from datetime import datetime, date
from pyspark.sql import SparkSession
import logging

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.config import ETLConfig


class ETLOrchestrator:
    """Orchestrates the complete ETL process."""
    
    def __init__(self, spark: SparkSession, config: ETLConfig):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            config: ETL configuration
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = SalesExtractor(spark, self.logger)
        self.transformer = SalesTransformer(spark, self.logger, config)
        self.loader = SalesLoader(spark, self.logger, config)
        
        self.start_time = None
        self.end_time = None
        
        self.logger.log_message(
            step=config.steps.INIT,
            status=config.status.SUCCESS,
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{self.config.get_etl_prefix()}{timestamp}"
    
    def run_etl(
        self,
        from_date: date,
        to_date: date,
        source_