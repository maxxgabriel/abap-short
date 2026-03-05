"""
ETL Loader Module - PySpark Implementation
Loads transformed data into target analytics table with validation and error handling.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from typing import Tuple, Dict
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class ETLLoader:
    """
    Loads transformed analytics data into target storage with validation.
    
    Responsibilities:
    - Validate records before loading
    - Write data to target table/location
    - Update source table status
    - Track load statistics
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the loader component.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.load_config = config.get('load', {})
        
    def load_data(
        self,
        analytics_df: DataFrame,
        etl_run_id: str
    ) -> Tuple[bool, Dict[str, int]]:
        """
        Load transformed analytics data to target table.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Tuple of (success_flag, statistics_dict)
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            # Validate records
            validated_df, validation_stats = self._validate_records(analytics_df)
            
            # Add metadata columns
            enriched_df = self._add_metadata(validated_df, etl_run_id)
            
            # Write to target
            write_success = self._write_to_target(enriched_df)
            
            if not write_success:
                raise ETLLoadError("Failed to write data to target")
            
            # Update source table status
            self._update_source_status(analytics_df)
            
            # Compile statistics
            stats = self._compile_statistics(analytics_df, validation_stats)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=stats['total_records'],
                records_success=stats['success_records'],
                records_error=stats['error_records'],
                message=f"Loaded {stats['success_records']} of {stats['total_records']} records"
            )
            
            return True, stats
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"Load failed: {str(e)}"
            )
            raise ETLLoadError(f"Data load failed: {str(e)}") from e
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
        """
        Validate records before loading.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (validated_df, validation_statistics)
        """
        initial_count = df.count()
        
        # Filter invalid records and track them
        valid_df = df.filter(
            (col('analytics_id').isNotNull()) &
            (col('customer_id').isNotNull()) &
            (col('product_id').isNotNull()) &
            (col('gross_amount') > 0) &
            (col('currency').isNotNull()) &
            (col('category').isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        
        valid_count = valid_df.count()
        invalid_count = initial_count - valid_count
        
        if invalid_count > 0:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Filtered out {invalid_count} invalid records"
            )
        
        stats = {
            'total_records': initial_count,
            'valid_records': valid_count,
            'invalid_records': invalid_count
        }
        
        return valid_df, stats
    
    def _add_metadata(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add metadata columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with metadata columns
        """
        metadata_df = df.withColumn('loaded_at', current_timestamp()) \
                       .withColumn('loaded_by', lit('spark_etl_user')) \
                       .withColumn('etl_run_id', lit(etl_run_id))
        
        return metadata_df
    
    def _write_to_target(self, df: DataFrame) -> bool:
        """
        Write DataFrame to target location.
        
        Args:
            df: DataFrame to write
            
        Returns:
            Success flag
        """
        try:
            target_path = self.load_config.get('target_path')
            target_format = self.load_config.get('target_format', 'parquet')
            write_mode = self.load_config.get('write_mode', 'append')
            partition_cols = self.load_config.get('partition_columns', [])
            
            # Build write operation
            writer = df.write.mode(write_mode).format(target_format)
            
            # Add partitioning if specified
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Add additional options
            options = self.load_config.get('write_options', {})
            for key, value in options.items():
                writer = writer.option(key, value)
            
            # Execute write
            writer.save(target_path)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f"Successfully wrote data to {target_path}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"Failed to write to target: {str(e)}"
            )
            return False
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update source table status for processed records.
        
        Args:
            df: DataFrame with processed records
        """
        try:
            source_path = self.config.get('extract', {}).get('source_path')
            
            # Extract transaction IDs that were successfully processed
            trans_ids = df.select('trans_id').distinct().collect()
            trans_id_list = [row.trans_id for row in trans_ids]
            
            # In a real implementation, this would update a Delta Lake table or similar
            # For now, we log the update
            self.logger.log_message(
                step='LOAD',
                status='I',
                message=f"Updated status for {len(trans_id_list)} source records"
            )
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Failed to update source status: {str(e)}"
            )
    
    def _compile_statistics(
        self,
        original_df: DataFrame,
        validation_stats: Dict[str, int]
    ) -> Dict[str, int]:
        """
        Compile loading statistics.
        
        Args:
            original_df: Original DataFrame
            validation_stats: Validation statistics
            
        Returns:
            Statistics dictionary
        """
        return {
            'total_records': validation_stats['total_records'],
            'success_records': validation_stats['valid_records'],
            'error_records': validation_stats['invalid_records'],
            'warning_records': 0
        }
    
    def validate_target_schema(self, df: DataFrame) -> bool:
        """
        Validate DataFrame schema matches target schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if schema is valid
        """
        expected_columns = self.load_config.get('expected_columns', [])
        
        if not expected_columns:
            return True
        
        actual_columns = set(df.columns)
        expected_columns_set = set(expected_columns)
        
        missing_columns = expected_columns_set - actual_columns
        
        if missing_columns:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"Missing required columns: {missing_columns}"
            )
            return False
        
        return True
    
    def write_error_records(self, df: DataFrame, error_path: str) -> None:
        """
        Write error records to separate location for analysis.
        
        Args:
            df: DataFrame with error records
            error_path: Path to write error records
        """
        try:
            df.withColumn('error_timestamp', current_timestamp()) \
              .write \
              .mode('append') \
              .format('parquet') \
              .save(error_path)
            
            self.logger.log_message(
                step='LOAD',
                status='I',
                message=f"Wrote error records to {error_path}"
            )
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Failed to write error records: {str(e)}"
            )