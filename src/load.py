"""
Data Loading Module
Loads transformed analytics data into target table
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple
import logging

from src.exceptions import ETLLoadError
from src.logger import ETLLogger


class SalesDataLoader:
    """Loads analytics data into target table"""
    
    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        config: dict
    ):
        """
        Initialize the loader
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.target_config = config.get('target', {})
        
    def load_data(
        self,
        analytics_df: DataFrame
    ) -> Tuple[bool, int, int]:
        """
        Load analytics data to target table
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (success_flag, success_count, error_count)
            
        Raises:
            ETLLoadError: If load fails
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            input_count = analytics_df.count()
            
            # Validate before loading
            validated_df = self._validate_before_load(analytics_df)
            validated_count = validated_df.count()
            error_count = input_count - validated_count
            
            if error_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message=f'{error_count} records failed validation'
                )
            
            # Write to target
            self._write_to_target(validated_df)
            
            # Update source status (if configured)
            # self._update_source_status(validated_df)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=input_count,
                records_success=validated_count,
                records_error=error_count,
                message=f'Loaded {validated_count} of {input_count} records'
            )
            
            return True, validated_count, error_count
            
        except Exception as e:
            error_msg = f'Load failed: {str(e)}'
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=error_msg
            )
            logging.error(error_msg, exc_info=True)
            raise ETLLoadError(error_msg) from e
    
    def _validate_before_load(self, df: DataFrame) -> DataFrame:
        """
        Final validation before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        from pyspark.sql.functions import col
        
        # Filter records that pass all validations
        validated = df.filter(
            (col('analytics_id').isNotNull()) &
            (col('trans_date').isNotNull()) &
            (col('customer_id').isNotNull()) &
            (col('product_id').isNotNull()) &
            (col('total_quantity') > 0) &
            (col('gross_amount') > 0) &
            (col('net_amount') > 0) &
            (col('currency').isNotNull()) &
            (col('category').isin(['HIGH', 'MEDIUM', 'LOW'])) &
            (col('etl_run_id').isNotNull())
        )
        
        return validated
    
    def _write_to_target(self, df: DataFrame) -> None:
        """
        Write DataFrame to target table
        
        Args:
            df: DataFrame to write
            
        Raises:
            ETLLoadError: If write fails
        """
        try:
            target_table = self.target_config.get('table', 'ZSALES_ANALYTICS')
            write_mode = self.target_config.get('mode', 'append')
            batch_size = self.target_config.get('batch_size', 500)
            
            jdbc_options = {
                'url': self.target_config.get('url'),
                'driver': self.target_config.get('driver'),
                'dbtable': target_table,
                'user': self.target_config.get('user'),
                'password': self.target_config.get('password'),
                'batchsize': str(batch_size)
            }
            
            df.write \
                .format('jdbc') \
                .options(**jdbc_options) \
                .mode(write_mode) \
                .save()
            
            logging.info(f'Successfully wrote to {target_table}')
            
        except Exception as e:
            raise ETLLoadError(f"Failed to write to target: {str(e)}") from e
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update source table status to 'P' (Processed)
        
        Args:
            df: DataFrame with processed records
        """
        try:
            # Extract transaction IDs
            trans_ids = [row.trans_id for row in 
                        df.select('trans_id').distinct().collect()]
            
            if not trans_ids:
                return
            
            # Build UPDATE query
            source_table = self.config.get('source', {}).get('table', 'ZSALES_RAW')
            ids_str = "','".join(trans_ids)
            
            update_query = f"""
                UPDATE {source_table}
                SET status = 'P'
                WHERE trans_id IN ('{ids_str}')
            """
            
            logging.info(f'Updating source status for {len(trans_ids)} records')
            
            # Execute update (implementation depends on database)
            # This is a simplified example
            
        except Exception as e:
            logging.warning(f"Failed to update source status: {str(e)}")
            # Don't raise - this is a non-critical operation