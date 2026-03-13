"""
Data loading module for Sales ETL process.
Loads transformed analytics data to target tables.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Tuple
import logging


class SalesDataLoader:
    """Loads transformed sales analytics data to target storage."""
    
    def __init__(self, spark, config: dict, logger: logging.Logger):
        """
        Initialize loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def load_analytics_data(
        self, 
        analytics_df: DataFrame,
        target_path: str = None
    ) -> Tuple[bool, dict]:
        """
        Load analytics data to target storage.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional override for target path
            
        Returns:
            Tuple of (success status, load statistics)
        """
        try:
            self.logger.info("Starting data load")
            
            # Get target configuration
            target_config = self.config.get('target', {})
            output_path = target_path or target_config.get('analytics_table_path')
            output_format = target_config.get('format', 'parquet')
            write_mode = target_config.get('write_mode', 'append')
            
            record_count = analytics_df.count()
            
            # Validate before loading
            if not self._validate_before_load(analytics_df):
                raise ValueError("Pre-load validation failed")
            
            # Write data based on format
            if output_format == 'parquet':
                self._write_parquet(analytics_df, output_path, write_mode)
            elif output_format == 'jdbc':
                self._write_jdbc(analytics_df)
            elif output_format == 'delta':
                self._write_delta(analytics_df, output_path, write_mode)
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            
            stats = {
                'records_loaded': record_count,
                'target_path': output_path,
                'write_mode': write_mode,
                'format': output_format
            }
            
            self.logger.info(f"Loaded {record_count} records successfully to {output_path}")
            
            return True, stats
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return False, {'error': str(e)}
    
    def _validate_before_load(self, df: DataFrame) -> bool:
        """Validate data before loading."""
        try:
            # Check for duplicate analytics IDs
            total_records = df.count()
            distinct_ids = df.select('analytics_id').distinct().count()
            
            if total_records != distinct_ids:
                self.logger.error(f"Duplicate analytics IDs found: {total_records - distinct_ids}")
                return False
            
            # Check for required fields
            required_fields = ['analytics_id', 'customer_id', 'product_id']
            for field in required_fields:
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    self.logger.error(f"Null values found in required field {field}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            return False
    
    def _write_parquet(self, df: DataFrame, path: str, mode: str):
        """Write data to Parquet format."""
        partition_columns = self.config.get('target', {}).get('partition_columns', ['trans_date'])
        
        df.write.mode(mode).partitionBy(*partition_columns).parquet(path)
        self.logger.info(f"Data written to Parquet: {path}")
    
    def _write_jdbc(self, df: DataFrame):
        """Write data to JDBC target."""
        jdbc_config = self.config.get('jdbc', {})
        target_table = jdbc_config.get('target_table', 'ZSALES_ANALYTICS')
        
        df.write.jdbc(
            url=jdbc_config['url'],
            table=target_table,
            mode='append',
            properties={
                'user': jdbc_config['user'],
                'password': jdbc_config['password'],
                'driver': jdbc_config.get('driver', 'com.sap.db.jdbc.Driver'),
                'batchsize': jdbc_config.get('batch_size', 1000)
            }
        )
        
        self.logger.info(f"Data written to JDBC table: {target_table}")
    
    def _write_delta(self, df: DataFrame, path: str, mode: str):
        """Write data to Delta Lake format."""
        df.write.format('delta').mode(mode).save(path)
        self.logger.info(f"Data written to Delta: {path}")
    
    def update_source_status(self, trans_ids: list) -> bool:
        """
        Update status of processed records in source table.
        
        Args:
            trans_ids: List of transaction IDs that were processed
            
        Returns:
            Success status
        """
        try:
            # For JDBC sources, update status
            jdbc_config = self.config.get('jdbc', {})
            if jdbc_config:
                source_table = jdbc_config.get('source_table', 'ZSALES_RAW')
                
                # In production, execute UPDATE statement
                # This is a placeholder for the actual update logic
                self.logger.info(f"Would update {len(trans_ids)} records in {source_table} to status 'P'")
                
                # Actual JDBC update would be done through JDBC connection
                # or by using Delta Lake MERGE operation
                
            return True
            
        except Exception as e:
            self.logger.error(f"Status update failed: {str(e)}")
            return False


def create_loader(spark, config: dict, logger: logging.Logger) -> SalesDataLoader:
    """Factory function to create loader instance."""
    return SalesDataLoader(spark, config, logger)