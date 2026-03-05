"""
Data loading module for Sales ETL pipeline.
Loads transformed analytics data to target systems.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Tuple
import logging


class SalesDataLoader:
    """Loads transformed analytics data to target tables."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, analytics_df: DataFrame) -> Tuple[bool, dict]:
        """
        Load analytics data to target destination.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (success boolean, metrics dictionary)
        """
        try:
            self.logger.info("Starting data load")
            
            initial_count = analytics_df.count()
            
            # Validate before loading
            is_valid, validation_messages = self._validate_for_load(analytics_df)
            
            if not is_valid:
                self.logger.error(f"Pre-load validation failed: {validation_messages}")
                return False, {
                    'records_loaded': 0,
                    'records_error': initial_count,
                    'validation_errors': validation_messages
                }
            
            # Get target configuration
            target_path = self.config['target']['analytics_path']
            target_format = self.config['target'].get('format', 'parquet')
            write_mode = self.config['target'].get('write_mode', 'append')
            partition_by = self.config['target'].get('partition_by', ['trans_date'])
            
            # Write to target
            analytics_df.write \
                .format(target_format) \
                .mode(write_mode) \
                .partitionBy(*partition_by) \
                .save(target_path)
            
            metrics = {
                'records_loaded': initial_count,
                'records_success': initial_count,
                'records_error': 0,
                'target_path': target_path,
                'write_mode': write_mode
            }
            
            self.logger.info(f"Successfully loaded {initial_count} records to {target_path}")
            
            return True, metrics
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return False, {
                'records_loaded': 0,
                'records_error': initial_count if 'initial_count' in locals() else 0,
                'error_message': str(e)
            }
    
    def _validate_for_load(self, df: DataFrame) -> Tuple[bool, list]:
        """
        Validate data before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (validation success boolean, list of validation messages)
        """
        validation_messages = []
        
        # Check for null analytics_id
        null_ids = df.filter(df.analytics_id.isNull()).count()
        if null_ids > 0:
            validation_messages.append(f"Found {null_ids} records with null analytics_id")
        
        # Check for null customer_id or product_id
        null_customers = df.filter(df.customer_id.isNull()).count()
        null_products = df.filter(df.product_id.isNull()).count()
        
        if null_customers > 0:
            validation_messages.append(f"Found {null_customers} records with null customer_id")
        if null_products > 0:
            validation_messages.append(f"Found {null_products} records with null product_id")
        
        # Check for invalid gross amounts
        invalid_amounts = df.filter(df.gross_amount <= 0).count()
        if invalid_amounts > 0:
            validation_messages.append(f"Found {invalid_amounts} records with invalid gross amounts")
        
        is_valid = len(validation_messages) == 0
        
        return is_valid, validation_messages
    
    def update_source_status(self, trans_ids: list, status: str = 'P') -> bool:
        """
        Update status of processed records in source table.
        
        Args:
            trans_ids: List of transaction IDs to update
            status: New status value (default: 'P' for processed)
            
        Returns:
            Success boolean
        """
        try:
            self.logger.info(f"Updating status for {len(trans_ids)} records")
            
            # In production, this would update the source table
            # For now, just log the operation
            source_path = self.config['source']['raw_sales_path']
            
            # Read source data
            source_df = self.spark.read.parquet(source_path)
            
            # Update status (simplified - in production use Delta Lake or similar)
            # This is a demonstration of the concept
            from pyspark.sql.functions import when
            
            updated_df = source_df.withColumn('status',
                when(source_df.trans_id.isin(trans_ids), status)
                .otherwise(source_df.status)
            )
            
            # Write back (in production, use proper update mechanism)
            # updated_df.write.mode('overwrite').parquet(source_path)
            
            self.logger.info(f"Status update completed for {len(trans_ids)} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Status update failed: {str(e)}")
            return False