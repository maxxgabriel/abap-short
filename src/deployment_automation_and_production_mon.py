===FILE: src/orchestrator.py===
"""
ETL Orchestrator - Main coordination module for Sales ETL process.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.config import ETLConfig
from src.monitoring import MonitoringManager


class ETLOrchestrator:
    """
    Orchestrates the complete ETL pipeline with monitoring and error handling.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the ETL orchestrator.

        Args:
            config_path: Path to configuration file
        """
        self.config = ETLConfig(config_path)
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id, self.config)
        self.monitoring = MonitoringManager(self.config, self.logger)
        
        self.spark = self._initialize_spark()
        self.extractor = SalesExtractor(self.spark, self.logger, self.config)
        self.transformer = SalesTransformer(self.spark, self.logger, self.config)
        self.loader = SalesLoader(self.spark, self.logger, self.config)
        
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with configuration."""
        spark_config = self.config.get('spark', {})
        
        builder = (SparkSession.builder
                   .appName(f"SalesETL_{self.etl_run_id}")
                   .config("spark.sql.shuffle.partitions", 
                          spark_config.get('shuffle_partitions', 200))
                   .config("spark.sql.adaptive.enabled", 
                          spark_config.get('adaptive_enabled', True)))
        
        # Add additional Spark configurations
        for key, value in spark_config.get('additional_configs', {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'Spark session initialized with app ID: {spark.sparkContext.applicationId}'
        )
        
        return spark

    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL pipeline.

        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            True if successful, False otherwise
        """
        self.start_time = datetime.now()
        success = False
        
        try:
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            
            # Start monitoring
            self.monitoring.start_monitoring(self.etl_run_id, from_date, to_date)
            
            # Step 1: Extract
            print("\n=== EXTRACT Phase ===")
            raw_data = self.extractor.extract_data(from_date, to_date)
            if raw_data is None or raw_data.count() == 0:
                raise RuntimeError("Extraction failed or no data found")
            
            self.monitoring.record_metric('extracted_records', raw_data.count())
            
            # Step 2: Transform
            print("\n=== TRANSFORM Phase ===")
            analytics_data = self.transformer.transform_data(raw_data)
            if analytics_data is None or analytics_data.count() == 0:
                raise RuntimeError("Transformation failed or no data produced")
            
            self.monitoring.record_metric('transformed_records', analytics_data.count())
            
            # Step 3: Load
            print("\n=== LOAD Phase ===")
            load_success = self.loader.load_data(analytics_data, self.etl_run_id)
            if not load_success:
                raise RuntimeError("Load operation failed")
            
            self.monitoring.record_metric('loaded_records', analytics_data.count())
            
            # Complete
            self.end_time = datetime.now()
            success = True
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully at {self.end_time.isoformat()}'
            )
            
            self.monitoring.end_monitoring(success=True)
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            self.monitoring.end_monitoring(success=False, error=str(e))
            logging.exception("ETL process failed")
            
        finally:
            # Publish final metrics
            self._publish_final_metrics()
        
        return success

    def _publish_final_metrics(self):
        """Publish final metrics and statistics."""
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            self.monitoring.record_metric('duration_seconds', duration)

    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id

    def display_summary(self) -> Dict[str, Any]:
        """
        Display and return ETL execution summary.

        Returns:
            Dictionary with summary information
        """
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': None,
            'metrics': self.monitoring.get_metrics()
        }
        
        if self.start_time and self.end_time:
            summary['duration_seconds'] = (self.end_time - self.start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {summary['etl_run_id']}")
        print(f"Start Time:    {summary['start_time']}")
        print(f"End Time:      {summary['end_time']}")
        print(f"Duration:      {summary['duration_seconds']} seconds")
        print("=" * 60)
        
        return summary

    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
        self.monitoring.cleanup()


===FILE: src/extract.py===
"""
Data extraction module for Sales ETL.
"""
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesExtractor:
    """
    Extracts raw sales data from source systems.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the extractor.

        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: ETLConfig instance
        """
        self.spark = spark
        self.logger = logger
        self.config = config

    def _get_raw_sales_schema(self) -> StructType:
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
            StructField("status", StringType(), nullable=False)
        ])

    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data from source.

        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with raw sales data or None on failure
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )

            source_config = self.config.get('source', {})
            source_type = source_config.get('type', 'jdbc')
            
            if source_type == 'jdbc':
                df = self._extract_from_jdbc(from_date, to_date)
            elif source_type == 'file':
                df = self._extract_from_file(from_date, to_date)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            if df is None:
                return None

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

    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """Extract data from JDBC source."""
        source_config = self.config.get('source', {})
        jdbc_config = source_config.get('jdbc', {})
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config.get('url'))
              .option("dbtable", jdbc_config.get('table', 'ZSALES_RAW'))
              .option("user", jdbc_config.get('user'))
              .option("password", jdbc_config.get('password'))
              .option("driver", jdbc_config.get('driver', 'com.ibm.db2.jcc.DB2Driver'))
              .load())
        
        # Filter by date range and status
        df = df.filter(
            (df.trans_date.between(from_date, to_date)) &
            (df.status == 'N')
        )
        
        return df

    def _extract_from_file(self, from_date: str, to_date: str) -> DataFrame:
        """Extract data from file source."""
        source_config = self.config.get('source', {})
        file_config = source_config.get('file', {})
        
        file_path = file_config.get('path')
        file_format = file_config.get('format', 'parquet')
        
        df = (self.spark.read
              .format(file_format)
              .option("header", "true")
              .schema(self._get_raw_sales_schema())
              .load(file_path))
        
        # Filter by date range and status
        df = df.filter(
            (df.trans_date.between(from_date, to_date)) &
            (df.status == 'N')
        )
        
        return df


===FILE: src/transform.py===
"""
Data transformation module for Sales ETL.
"""
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesTransformer:
    """
    Transforms raw sales data into analytics format.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the transformer.

        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: ETLConfig instance
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Load business rules from config
        rules = self.config.get('business_rules', {})
        self.discount_qty_tier1 = rules.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = rules.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = rules.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = rules.get('discount_rate_tier2', 0.10)
        self.tax_rate = rules.get('tax_rate', 0.08)
        self.cost_ratio = rules.get('cost_ratio', 0.60)
        self.category_high_threshold = rules.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = rules.get('category_medium_threshold', 500.00)

    def _get_analytics_schema(self) -> StructType:
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
            StructField("etl_run_id", StringType(), nullable=False)
        ])

    def transform_data(self, raw_data: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_data: DataFrame with raw sales data

        Returns:
            DataFrame with analytics data or None on failure
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )

            initial_count = raw_data.count()
            
            # Calculate gross amount
            df = raw_data.withColumn(
                'gross_amount',
                F.col('quantity') * F.col('unit_price')
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                'discount_amount',
                F.when(F.col('quantity') > self.discount_qty_tier2,
                      F.col('gross_amount') * self.discount_rate_tier2)
                .when(F.col('quantity') > self.discount_qty_tier1,
                      F.col('gross_amount') * self.discount_rate_tier1)
                .otherwise(F.lit(0.0))
            )
            
            # Calculate tax
            df = df.withColumn(
                'tax_amount',
                (F.col('gross_amount') - F.col('discount_amount')) * self.tax_rate
            )
            
            # Calculate net amount
            df = df.withColumn(
                'net_amount',
                F.col('gross_amount') - F.col('discount_amount') + F.col('tax_amount')
            )
            
            # Calculate profit margin
            df = df.withColumn(
                'cost_amount',
                F.col('quantity') * F.col('unit_price') * self.cost_ratio
            )
            
            df = df.withColumn(
                'profit_margin',
                F.when(F.col('net_amount') > 0,
                      ((F.col('net_amount') - F.col('cost_amount')) / F.col('net_amount')) * 100)
                .otherwise(F.lit(0.0))
            )
            
            # Categorize sales
            df = df.withColumn(
                'category',
                F.when(F.col('gross_amount') >= self.category_high_threshold, 'HIGH')
                .when(F.col('gross_amount') >= self.category_medium_threshold, 'MEDIUM')
                .otherwise('LOW')
            )
            
            # Generate analytics ID
            df = df.withColumn(
                'analytics_id',
                F.concat(
                    F.lit('ANL'),
                    F.col('trans_id'),
                    F.date_format(F.current_timestamp(), 'HHmmss')
                )
            )
            
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
                'category'
            )
            
            # Add ETL run ID (will be added during load)
            # analytics_df = analytics_df.withColumn('etl_run_id', F.lit(etl_run_id))
            
            final_count = analytics_df.count()
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=initial_count,
                records_success=final_count,
                message=f'Transformed {final_count} of {initial_count} records'
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
Data loading module for Sales ETL.
"""
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesLoader:
    """
    Loads transformed data into target analytics table.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the loader.

        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: ETLConfig instance
        """
        self.spark = spark
        self.logger = logger
        self.config = config

    def load_data(self, analytics_data: DataFrame, etl_run_id: str) -> bool:
        """
        Load analytics data into target.

        Args:
            analytics_data: DataFrame with analytics data
            etl_run_id: ETL run identifier

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )

            # Add ETL run ID
            df = analytics_data.withColumn('etl_run_id', F.lit(etl_run_id))
            
            # Validate records
            validated_df = self._validate_records(df)
            
            record_count = validated_df.count()
            
            # Load to target
            target_config = self.config.get('target', {})
            target_type = target_config.get('type', 'jdbc')
            
            if target_type == 'jdbc':
                self._load_to_jdbc(validated_df)
            elif target_type == 'file':
                self._load_to_file(validated_df)
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Loaded {record_count} records successfully'
            )

            return True

        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            return False

    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame
        """
        # Filter out invalid records
        validated = df.filter(
            (F.col('analytics_id').isNotNull()) &
            (F.col('customer_id').isNotNull()) &
            (F.col('product_id').isNotNull()) &
            (F.col('gross_amount') > 0) &
            (F.col('currency').isNotNull()) &
            (F.col('category').isin('HIGH', 'MEDIUM', 'LOW'))
        )
        
        invalid_count = df.count() - validated.count()
        if invalid_count > 0:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f'Filtered out {invalid_count} invalid records'
            )
        
        return validated

    def _load_to_jdbc(self, df: DataFrame):
        """Load data to JDBC target."""
        target_config = self.config.get('target', {})
        jdbc_config = target_config.get('jdbc', {})
        
        (df.write
         .format("jdbc")
         .option("url", jdbc_config.get('url'))
         .option("dbtable", jdbc_config.get('table', 'ZSALES_ANALYTICS'))
         .option("user", jdbc_config.get('user'))
         .option("password", jdbc_config.get('password'))
         .option("driver", jdbc_config.get('driver', 'com.ibm.db2.jcc.DB2Driver'))
         .mode(jdbc_config.get('mode', 'append'))
         .save())

    def _load_to_file(self, df: DataFrame):
        """Load data to file target."""
        target_config = self.config.get('target', {})
        file_config = target_config.get('file', {})
        
        file_path = file_config.get('path')
        file_format = file_config.get('format', 'parquet')
        
        (df.write
         .format(file_format)
         .mode(file_config.get('mode', 'append'))
         .option("compression", file_config.get('compression', 'snappy'))
         .save(file_path))


===FILE: src/logger.py===
"""
Logging utility for ETL process.
"""
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict
import json

from src.config import ETLConfig


@dataclass
class LogEntry:
    """Represents a log entry."""
    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int
    records_success: int
    records_error: int
    message: str


class ETLLogger:
    """
    Utility class for ETL logging with structured output.
    """

    def __init__(self, etl_run_id: str, config: ETLConfig):
        """
        Initialize the logger.

        Args:
            etl_run_id: ETL run identifier
            config: ETLConfig instance
        """
        self.etl_run_id = etl_run_id
        self.config = config
        self._setup_logging()

    def _setup_logging(self):
        """Configure Python logging."""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'etl_log_{self.etl_run_id}.log')
            ]
        )
        
        self.logger = logging.getLogger('SalesETL')

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
        Log a message with context.

        Args:
            step: Process step name
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y-%m-%d'),
            execution_time=now.strftime('%H:%M:%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        # Log to Python logger
        log_method = self._get_log_method(status)
        log_method(f"[{step}] {message}")
        
        # Store structured log
        self._persist_log(log_entry)

    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"

    def _get_log_method(self, status: str):
        """Get appropriate logging method based on status."""
        if status == 'E':
            return self.logger.error
        elif status == 'W':
            return self.logger.warning
        else:
            return self.logger.info

    def _persist_log(self, log_entry: LogEntry):
        """Persist log entry to storage."""
        # In production, write to database or log management system
        # For now, write to JSON file
        log_config = self.config.get('logging', {})
        if log_config.get('persist_logs', True):
            with open(f'etl_logs_{self.etl_run_id}.json', 'a') as f:
                json.dump(asdict(log_entry), f)
                f.write('\n')

    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.etl_run_id


===FILE: src/config.py===
"""
Configuration management for ETL process.
"""
import yaml
from typing import Any, Dict, Optional


class ETLConfig:
    """
    Manages ETL configuration from YAML file.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration.

        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Return default configuration
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'source': {
                'type': 'jdbc',
                'jdbc': {
                    'url': 'jdbc:db2://localhost:50000/SALES',
                    'table': 'ZSALES_RAW',
                    'user': 'etl_user',
                    'password': 'etl_password',
                    'driver': 'com.ibm.db2.jcc.DB2Driver'
                }
            },
            'target': {
                'type': 'jdbc',
                'jdbc': {
                    'url': 'jdbc:db2://localhost:50000/SALES',
                    'table': 'ZSALES_ANALYTICS',
                    'user': 'etl_user',
                    'password': 'etl_password',
                    'driver': 'com.ibm.db2.jcc.DB2Driver',
                    'mode': 'append'
                }
            },
            'business_rules': {
                'discount_qty_tier1': 10,
                'discount_qty_tier2': 15,
                'discount_rate_tier1': 0.05,
                'discount_rate_tier2': 0.10,
                'tax_rate': 0.08,
                'cost_ratio': 0.60,
                'category_high_threshold': 2000.00,
                'category_medium_threshold': 500.00
            },
            'spark': {
                'shuffle_partitions': 200,
                'adaptive_enabled': True
            },
            'logging': {
                'level': 'INFO',
                'persist_logs': True
            },
            'monitoring': {
                'enabled': True,
                'metrics_endpoint': 'http://localhost:9090/metrics',
                'dashboard_url': 'http://localhost:3000/d/etl-dashboard'
            }
        }

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        return self.config.get(key, default)

    def update(self, key: str, value: Any):
        """
        Update configuration value.

        Args:
            key: Configuration key
            value: New value
        """
        self.config[key] = value


===FILE: src/monitoring.py===
"""
Monitoring and metrics collection for ETL process.
"""
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import requests

from src.logger import ETLLogger
from src.config import ETLConfig


@dataclass
class ETLMetrics:
    """Represents ETL execution metrics."""
    etl_run_id: str
    start_time: str
    end_time: Optional[str]
    duration_seconds: Optional[float]
    extracted_records: int
    transformed_records: int
    loaded_records: int
    error_count: int