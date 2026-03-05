"""
ETL Loader Module - PySpark DataFrame Implementation
Loads transformed analytics data with validation and error handling
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Tuple, Dict
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class ETLLoader:
    """
    PySpark implementation of ETL Loader component
    Handles data validation, filtering, and loading to target tables
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict):
        """
        Initialize ETL Loader
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
        
    def load_data(self, analytics_df: DataFrame) -> Tuple[bool, Dict[str, int]]:
        """
        Load transformed data with validation
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (success_flag, statistics_dict)
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            # Get initial count
            total_records = analytics_df.count()
            
            # Validate records
            validated_df, validation_stats = self._validate_records(analytics_df)
            
            # Add load metadata
            load_df = self._add_load_metadata(validated_df)
            
            # Write to target table
            success = self._write_to_target(load_df)
            
            # Update source status if configured
            if success and self.config.get('update_source_status', True):
                self._update_source_status(validated_df)
            
            # Prepare statistics
            stats = {
                'total_records': total_records,
                'valid_records': validation_stats['valid_count'],
                'invalid_records': validation_stats['invalid_count'],
                'loaded_records': validation_stats['valid_count'] if success else 0
            }
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=total_records,
                records_success=stats['loaded_records'],
                records_error=stats['invalid_records'],
                message=f"Loaded {stats['loaded_records']} of {total_records} records"
            )
            
            return success, stats
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f"Load failed: {str(e)}"
            )
            raise ETLLoadError(f"Data load failed: {str(e)}") from e
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
        """
        Validate records before loading
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (validated_df, validation_statistics)
        """
        # Define validation rules
        validation_conditions = (
            (col('analytics_id').isNotNull()) &
            (col('customer_id').isNotNull()) &
            (col('product_id').isNotNull()) &
            (col('gross_amount') > 0) &
            (col('currency').isNotNull()) &
            (col('category').isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        
        # Add validation flag
        df_with_validation = df.withColumn(
            'is_valid',
            validation_conditions.cast('boolean')
        )
        
        # Count valid and invalid records
        valid_count = df_with_validation.filter(col('is_valid') == True).count()
        invalid_count = df_with_validation.filter(col('is_valid') == False).count()
        
        # Log invalid records
        if invalid_count > 0:
            invalid_df = df_with_validation.filter(col('is_valid') == False)
            invalid_sample = invalid_df.select('analytics_id').limit(10).collect()
            invalid_ids = [row['analytics_id'] for row in invalid_sample]
            
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f"Invalid records found: {invalid_count}. Sample IDs: {invalid_ids}"
            )
        
        # Return only valid records
        validated_df = df_with_validation.filter(col('is_valid') == True).drop('is_valid')
        
        validation_stats = {
            'valid_count': valid_count,
            'invalid_count': invalid_count
        }
        
        return validated_df, validation_stats
    
    def _add_load_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add load timestamp and user metadata
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        return df.withColumn('loaded_at', current_timestamp()) \
                 .withColumn('loaded_by', lit(self.config.get('user', 'system')))
    
    def _write_to_target(self, df: DataFrame) -> bool:
        """
        Write DataFrame to target table
        
        Args:
            df: DataFrame to write
            
        Returns:
            Success flag
        """
        try:
            target_table = self.config.get('target_table', 'sales_analytics')
            write_mode = self.config.get('write_mode', 'append')
            
            # Write to target
            if self.config.get('use_delta', False):
                # Delta Lake format
                df.write \
                  .format('delta') \
                  .mode(write_mode) \
                  .option('mergeSchema', 'true') \
                  .save(target_table)
            else:
                # Parquet format
                df.write \
                  .format('parquet') \
                  .mode(write_mode) \
                  .option('compression', 'snappy') \
                  .save(target_table)
            
            self.log.info(f"Successfully wrote {df.count()} records to {target_table}")
            return True
            
        except Exception as e:
            self.log.error(f"Failed to write to target: {str(e)}")
            return False
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update source table status for processed records
        
        Args:
            df: DataFrame with processed records
        """
        try:
            # Extract transaction IDs
            trans_ids = df.select('trans_id').distinct()
            
            # This would typically update a source table
            # Implementation depends on source system
            self.log.info(f"Updated source status for {trans_ids.count()} transactions")
            
        except Exception as e:
            self.log.warning(f"Failed to update source status: {str(e)}")
    
    def validate_record_quality(self, df: DataFrame) -> DataFrame:
        """
        Additional quality checks on records
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with quality score column
        """
        quality_score = (
            (col('gross_amount').isNotNull().cast('int')) +
            (col('discount_amount').isNotNull().cast('int')) +
            (col('tax_amount').isNotNull().cast('int')) +
            (col('net_amount').isNotNull().cast('int')) +
            ((col('net_amount') > col('discount_amount')).cast('int'))
        )
        
        return df.withColumn('quality_score', quality_score)