"""
ETL Loader Module
Loads transformed analytics data into target storage with validation and error handling.
"""

from typing import Dict, List, Tuple
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
import logging

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class ETLLoader:
    """
    Handles loading of transformed analytics data into target storage.
    
    Responsibilities:
    - Validate records before loading
    - Write data to target storage
    - Handle partitioning and bucketing
    - Manage write modes and error handling
    - Track load statistics
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict):
        """
        Initialize the ETL Loader.
        
        Args:
            spark: Active SparkSession
            logger: ETLLogger instance for logging
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self._load_stats = {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'loaded_records': 0,
            'failed_records': 0
        }
    
    def get_target_schema(self) -> StructType:
        """
        Define the target analytics table schema.
        
        Returns:
            StructType schema for analytics data
        """
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
            StructField("loaded_at", TimestampType(), nullable=False),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    def validate_records(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate records before loading.
        
        Validation rules:
        - Required fields must not be null
        - Gross amount must be > 0
        - Currency must be valid
        - Category must be in allowed values
        - Numeric fields must be within valid ranges
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        self.logger.log_message(
            step='LOAD',
            status='INFO',
            message='Starting record validation'
        )
        
        try:
            # Add validation flag column
            df_with_validation = df.withColumn(
                'is_valid',
                F.when(
                    # Check required fields are not null
                    (F.col('analytics_id').isNotNull()) &
                    (F.col('customer_id').isNotNull()) &
                    (F.col('product_id').isNotNull()) &
                    (F.col('gross_amount').isNotNull()) &
                    (F.col('currency').isNotNull()) &
                    (F.col('category').isNotNull()) &
                    # Check numeric validations
                    (F.col('gross_amount') > 0) &
                    (F.col('net_amount') >= 0) &
                    (F.col('discount_amount') >= 0) &
                    (F.col('tax_amount') >= 0) &
                    (F.col('total_quantity') > 0) &
                    # Check category values
                    (F.col('category').isin(['HIGH', 'MEDIUM', 'LOW'])) &
                    # Check currency is not empty
                    (F.length(F.col('currency')) > 0),
                    F.lit(True)
                ).otherwise(F.lit(False))
            )
            
            # Add validation error details
            df_with_validation = df_with_validation.withColumn(
                'validation_errors',
                F.when(~F.col('is_valid'),
                    F.concat_ws(', ',
                        F.when(F.col('analytics_id').isNull(), F.lit('analytics_id is null')),
                        F.when(F.col('customer_id').isNull(), F.lit('customer_id is null')),
                        F.when(F.col('product_id').isNull(), F.lit('product_id is null')),
                        F.when(F.col('gross_amount').isNull() | (F.col('gross_amount') <= 0), 
                               F.lit('invalid gross_amount')),
                        F.when(F.col('currency').isNull() | (F.length(F.col('currency')) == 0), 
                               F.lit('invalid currency')),
                        F.when(~F.col('category').isin(['HIGH', 'MEDIUM', 'LOW']), 
                               F.lit('invalid category'))
                    )
                )
            )
            
            # Split into valid and invalid DataFrames
            valid_df = df_with_validation.filter(F.col('is_valid')).drop('is_valid', 'validation_errors')
            invalid_df = df_with_validation.filter(~F.col('is_valid'))
            
            # Update statistics
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            
            self._load_stats['valid_records'] = valid_count
            self._load_stats['invalid_records'] = invalid_count
            
            self.logger.log_message(
                step='LOAD',
                status='SUCCESS',
                records_processed=valid_count + invalid_count,
                records_success=valid_count,
                records_error=invalid_count,
                message=f'Validation completed: {valid_count} valid, {invalid_count} invalid'
            )
            
            # Log invalid records if any
            if invalid_count > 0:
                self._log_invalid_records(invalid_df)
            
            return valid_df, invalid_df
            
        except Exception as e:
            error_msg = f"Validation failed: {str(e)}"
            self.logger.log_message(
                step='LOAD',
                status='ERROR',
                message=error_msg
            )
            raise ETLLoadError(error_msg) from e
    
    def _log_invalid_records(self, invalid_df: DataFrame, sample_size: int = 10):
        """
        Log sample of invalid records for debugging.
        
        Args:
            invalid_df: DataFrame containing invalid records
            sample_size: Number of sample records to log
        """
        try:
            sample_records = invalid_df.select(
                'analytics_id', 'customer_id', 'product_id', 'validation_errors'
            ).limit(sample_size).collect()
            
            for record in sample_records:
                self.logger.log_message(
                    step='LOAD',
                    status='WARNING',
                    message=f"Invalid record {record.analytics_id}: {record.validation_errors}"
                )
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='WARNING',
                message=f"Could not log invalid records: {str(e)}"
            )
    
    def add_metadata(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add metadata columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with metadata columns added
        """
        return df \
            .withColumn('etl_run_id', F.lit(etl_run_id)) \
            .withColumn('loaded_at', F.current_timestamp()) \
            .withColumn('loaded_by', F.lit(self.config.get('system_user', 'spark_etl')))
    
    def load_data(self, df: DataFrame, etl_run_id: str) -> bool:
        """
        Load validated analytics data to target storage.
        
        Args:
            df: DataFrame to load
            etl_run_id: ETL run identifier
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='INFO',
                message='Starting data load'
            )
            
            # Update statistics
            self._load_stats['total_records'] = df.count()
            
            # Validate records
            valid_df, invalid_df = self.validate_records(df)
            
            if valid_df.count() == 0:
                self.logger.log_message(
                    step='LOAD',
                    status='WARNING',
                    message='No valid records to load'
                )
                return False
            
            # Add metadata
            df_with_metadata = self.add_metadata(valid_df, etl_run_id)
            
            # Get target configuration
            target_config = self.config.get('target', {})
            target_path = target_config.get('path')
            target_format = target_config.get('format', 'parquet')
            write_mode = target_config.get('write_mode', 'append')
            
            # Perform the write operation
            success = self._write_to_target(
                df_with_metadata,
                target_path,
                target_format,
                write_mode,
                target_config
            )
            
            if success:
                self._load_stats['loaded_records'] = valid_df.count()
                
                self.logger.log_message(
                    step='LOAD',
                    status='SUCCESS',
                    records_processed=self._load_stats['total_records'],
                    records_success=self._load_stats['loaded_records'],
                    records_error=self._load_stats['invalid_records'],
                    message=f"Successfully loaded {self._load_stats['loaded_records']} records"
                )
                
                # Handle invalid records
                if invalid_df.count() > 0:
                    self._handle_invalid_records(invalid_df, etl_run_id)
                
                return True
            else:
                return False
                
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.logger.log_message(
                step='LOAD',
                status='ERROR',
                message=error_msg
            )
            raise ETLLoadError(error_msg) from e
    
    def _write_to_target(
        self,
        df: DataFrame,
        target_path: str,
        target_format: str,
        write_mode: str,
        config: Dict
    ) -> bool:
        """
        Write DataFrame to target storage.
        
        Args:
            df: DataFrame to write
            target_path: Target path
            target_format: Output format (parquet, delta, etc.)
            write_mode: Write mode (append, overwrite, etc.)
            config: Additional configuration
            
        Returns:
            True if successful, False otherwise
        """
        try:
            writer = df.write.mode(write_mode)
            
            # Apply partitioning if configured
            partition_columns = config.get('partition_by', [])
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
                self.logger.log_message(
                    step='LOAD',
                    status='INFO',
                    message=f"Partitioning by: {', '.join(partition_columns)}"
                )
            
            # Apply bucketing if configured
            bucket_config = config.get('bucket_by', {})
            if bucket_config:
                num_buckets = bucket_config.get('num_buckets')
                bucket_columns = bucket_config.get('columns', [])
                if num_buckets and bucket_columns:
                    writer = writer.bucketBy(num_buckets, *bucket_columns)
                    self.logger.log_message(
                        step='LOAD',
                        status='INFO',
                        message=f"Bucketing by {bucket_columns} into {num_buckets} buckets"
                    )
            
            # Apply compression if configured
            compression = config.get('compression')
            if compression:
                writer = writer.option('compression', compression)
            
            # Apply additional options
            options = config.get('options', {})
            for key, value in options.items():
                writer = writer.option(key, value)
            
            # Write based on format
            if target_format.lower() == 'delta':
                writer.format('delta').save(target_path)
            elif target_format.lower() == 'parquet':
                writer.parquet(target_path)
            elif target_format.lower() == 'orc':
                writer.orc(target_path)
            elif target_format.lower() == 'csv':
                writer.option('header', 'true').csv(target_path)
            else:
                writer.format(target_format).save(target_path)
            
            self.logger.log_message(
                step='LOAD',
                status='SUCCESS',
                message=f"Data written to {target_path} in {target_format} format"
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Write operation failed: {str(e)}"
            self.logger.log_message(
                step='LOAD',
                status='ERROR',
                message=error_msg
            )
            return False
    
    def _handle_invalid_records(self, invalid_df: DataFrame, etl_run_id: str):
        """
        Handle invalid records by writing to error storage.
        
        Args:
            invalid_df: DataFrame containing invalid records
            etl_run_id: ETL run identifier
        """
        try:
            error_config = self.config.get('error_handling', {})
            error_path = error_config.get('error_path')
            
            if error_path:
                # Add error metadata
                error_df = invalid_df \
                    .withColumn('etl_run_id', F.lit(etl_run_id)) \
                    .withColumn('error_timestamp', F.current_timestamp())
                
                # Write to error storage
                error_df.write \
                    .mode('append') \
                    .partitionBy('etl_run_id') \
                    .parquet(error_path)
                
                self.logger.log_message(
                    step='LOAD',
                    status='INFO',
                    message=f"Invalid records written to {error_path}"
                )
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='WARNING',
                message=f"Could not write invalid records: {str(e)}"
            )
    
    def get_load_statistics(self) -> Dict:
        """
        Get load statistics.
        
        Returns:
            Dictionary containing load statistics
        """
        return self._load_stats.copy()
    
    def optimize_target(self, target_path: str, target_format: str = 'delta'):
        """
        Optimize target storage (compaction, vacuum, etc.).
        
        Args:
            target_path: Path to target storage
            target_format: Storage format
        """
        try:
            if target_format.lower() == 'delta':
                self.logger.log_message(
                    step='LOAD',
                    status='INFO',
                    message=f"Optimizing Delta table at {target_path}"
                )
                
                # Use Delta Lake optimization
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Optimize
                delta_table.optimize().executeCompaction()
                
                # Vacuum old files (if configured)
                retention_hours = self.config.get('target', {}).get('vacuum_retention_hours', 168)
                delta_table.vacuum(retention_hours)
                
                self.logger.log_message(
                    step='LOAD',
                    status='SUCCESS',
                    message='Target optimization completed'
                )
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='WARNING',
                message=f"Optimization failed: {str(e)}"
            )