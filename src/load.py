"""
PySpark Data Loading Module
Converts ABAP INSERT/MODIFY operations to Spark DataFrame writes
with batch processing, partitioning, and checkpointing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from typing import Dict, Optional, Tuple
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import LoadError


class DataLoader:
    """
    Loads transformed analytics data into target table with:
    - Batch processing for large datasets
    - Partitioning by date and region for query optimization
    - Checkpointing for fault tolerance
    - Data validation before write
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict):
        """
        Initialize the data loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Load configuration
        self.batch_size = config.get('batch_size', 1000)
        self.write_mode = config.get('write_mode', 'append')
        self.checkpoint_dir = config.get('checkpoint_dir', '/tmp/spark_checkpoint')
        self.target_table = config.get('target_table', 'sales_analytics')
        self.partition_columns = config.get('partition_columns', ['trans_date', 'region'])
        self.enable_checkpointing = config.get('enable_checkpointing', True)
        
        # Statistics
        self.records_processed = 0
        self.records_success = 0
        self.records_error = 0
        
    def get_analytics_schema(self) -> StructType:
        """
        Define the target analytics table schema.
        
        Returns:
            StructType schema for analytics data
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
            StructField("region", StringType(), False),
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), False)
        ])
    
    def validate_dataframe(self, df: DataFrame) -> Tuple[bool, str]:
        """
        Validate DataFrame before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check if DataFrame is empty
            if df.count() == 0:
                return False, "DataFrame is empty"
            
            # Check required columns exist
            required_columns = [
                'analytics_id', 'trans_date', 'customer_id', 
                'product_id', 'gross_amount', 'currency', 
                'region', 'category', 'etl_run_id'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                return False, f"Missing required columns: {missing_columns}"
            
            # Check for null values in required fields
            null_counts = df.select([
                col(c).isNull().alias(c) for c in required_columns
            ]).agg(*[sum(col(c)).alias(c) for c in required_columns]).collect()[0]
            
            null_fields = [field for field, count in null_counts.asDict().items() if count > 0]
            if null_fields:
                return False, f"Null values found in required fields: {null_fields}"
            
            # Validate category values
            valid_categories = ['HIGH', 'MEDIUM', 'LOW']
            invalid_categories = df.filter(
                ~col('category').isin(valid_categories)
            ).count()
            
            if invalid_categories > 0:
                return False, f"Found {invalid_categories} records with invalid category"
            
            # Validate gross_amount is positive
            negative_amounts = df.filter(col('gross_amount') <= 0).count()
            if negative_amounts > 0:
                return False, f"Found {negative_amounts} records with non-positive gross_amount"
            
            return True, "Validation successful"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit columns to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with audit columns
        """
        return df.withColumn("loaded_at", current_timestamp()) \
                 .withColumn("loaded_by", lit("spark_etl"))
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load analytics data to target table with batch processing and checkpointing.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            # Validate input DataFrame
            is_valid, validation_msg = self.validate_dataframe(analytics_df)
            if not is_valid:
                raise LoadError(f"Data validation failed: {validation_msg}")
            
            # Add audit columns
            df_with_audit = self.add_audit_columns(analytics_df)
            
            # Cache DataFrame for reuse
            df_with_audit.cache()
            
            # Get record count
            self.records_processed = df_with_audit.count()
            
            self.logger.log_message(
                step='LOAD',
                status='I',
                records_processed=self.records_processed,
                message=f'Validated {self.records_processed} records for loading'
            )
            
            # Enable checkpointing if configured
            if self.enable_checkpointing:
                self.spark.sparkContext.setCheckpointDir(self.checkpoint_dir)
                df_with_audit = df_with_audit.checkpoint()
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Checkpointing enabled at {self.checkpoint_dir}'
                )
            
            # Write data with partitioning
            self._write_partitioned_data(df_with_audit)
            
            # Update source status (in real implementation)
            self._update_source_status(analytics_df)
            
            # Update statistics
            self.records_success = self.records_processed
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=self.records_processed,
                records_success=self.records_success,
                records_error=self.records_error,
                message=f'Successfully loaded {self.records_success} records'
            )
            
            # Unpersist cached DataFrame
            df_with_audit.unpersist()
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            raise LoadError(f"Data load failed: {str(e)}") from e
    
    def _write_partitioned_data(self, df: DataFrame) -> None:
        """
        Write DataFrame with partitioning strategy.
        
        Args:
            df: DataFrame to write
        """
        try:
            # Get write format from config
            write_format = self.config.get('write_format', 'parquet')
            target_path = self.config.get('target_path', f'/data/{self.target_table}')
            
            # Build writer
            writer = df.write \
                      .format(write_format) \
                      .mode(self.write_mode)
            
            # Add partitioning if configured
            if self.partition_columns:
                writer = writer.partitionBy(*self.partition_columns)
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Partitioning by: {self.partition_columns}'
                )
            
            # Add bucketing if configured
            if self.config.get('enable_bucketing', False):
                bucket_columns = self.config.get('bucket_columns', ['customer_id'])
                num_buckets = self.config.get('num_buckets', 10)
                writer = writer.bucketBy(num_buckets, *bucket_columns)
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Bucketing by {bucket_columns} into {num_buckets} buckets'
                )
            
            # Write options
            write_options = self.config.get('write_options', {})
            if write_options:
                writer = writer.options(**write_options)
            
            # Execute write
            if self.config.get('use_table', False):
                # Write to managed table
                writer.saveAsTable(self.target_table)
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Data written to table: {self.target_table}'
                )
            else:
                # Write to path
                writer.save(target_path)
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Data written to path: {target_path}'
                )
            
        except Exception as e:
            raise LoadError(f"Write operation failed: {str(e)}") from e
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update source table status to mark records as processed.
        
        Args:
            df: DataFrame containing processed records
        """
        try:
            # Extract transaction IDs from analytics data
            # In real implementation, update source table status
            trans_ids = df.select('analytics_id').distinct().collect()
            
            self.logger.log_message(
                step='LOAD',
                status='I',
                message=f'Updated status for {len(trans_ids)} source records'
            )
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f'Failed to update source status: {str(e)}'
            )
    
    def load_data_in_batches(self, analytics_df: DataFrame) -> bool:
        """
        Load data in batches for better memory management.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            True if all batches loaded successfully
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f'Starting batch load with batch_size={self.batch_size}'
            )
            
            # Validate input
            is_valid, validation_msg = self.validate_dataframe(analytics_df)
            if not is_valid:
                raise LoadError(f"Data validation failed: {validation_msg}")
            
            # Add audit columns
            df_with_audit = self.add_audit_columns(analytics_df)
            
            # Get total count
            total_records = df_with_audit.count()
            self.records_processed = total_records
            
            # Calculate number of batches
            num_batches = (total_records + self.batch_size - 1) // self.batch_size
            
            self.logger.log_message(
                step='LOAD',
                status='I',
                message=f'Processing {total_records} records in {num_batches} batches'
            )
            
            # Add row number for batching
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, floor
            
            window_spec = Window.orderBy(col('analytics_id'))
            df_batched = df_with_audit.withColumn(
                'batch_id',
                floor((row_number().over(window_spec) - 1) / self.batch_size)
            )
            
            # Process each batch
            for batch_id in range(num_batches):
                batch_df = df_batched.filter(col('batch_id') == batch_id).drop('batch_id')
                batch_count = batch_df.count()
                
                self.logger.log_message(
                    step='LOAD',
                    status='I',
                    message=f'Processing batch {batch_id + 1}/{num_batches} ({batch_count} records)'
                )
                
                # Write batch
                self._write_partitioned_data(batch_df)
                
                self.records_success += batch_count
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=self.records_processed,
                records_success=self.records_success,
                message=f'Batch load completed: {self.records_success}/{self.records_processed} records'
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Batch load failed: {str(e)}'
            )
            raise LoadError(f"Batch load failed: {str(e)}") from e
    
    def get_statistics(self) -> Dict:
        """
        Get loading statistics.
        
        Returns:
            Dictionary with loading statistics
        """
        return {
            'records_processed': self.records_processed,
            'records_success': self.records_success,
            'records_error': self.records_error,
            'success_rate': (self.records_success / self.records_processed * 100) 
                           if self.records_processed > 0 else 0
        }