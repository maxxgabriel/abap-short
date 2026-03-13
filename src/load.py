"""
Load module for Sales ETL process.
Loads transformed analytics data to target system.
"""
from pyspark.sql import DataFrame
import logging


class SalesLoader:
    """Handles loading of analytics data to target."""
    
    def __init__(self, config: dict):
        """
        Initialize loader.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        from pyspark.sql.functions import col
        
        # Validate required fields and business rules
        valid_df = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        return valid_df
    
    def load_data(self, analytics_df: DataFrame) -> int:
        """
        Load analytics data to target.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            Number of records successfully loaded
        """
        self.logger.info("Starting data load")
        
        try:
            # Validate records
            valid_df = self.validate_record(analytics_df)
            
            initial_count = analytics_df.count()
            valid_count = valid_df.count()
            invalid_count = initial_count - valid_count
            
            if invalid_count > 0:
                self.logger.warning(f"Skipped {invalid_count} invalid records")
            
            # Load to target
            target_path = self.config['load']['target_path']
            target_format = self.config['load']['target_format']
            write_mode = self.config['load']['write_mode']
            partition_by = self.config['load'].get('partition_by', [])
            
            writer = valid_df.write \
                .format(target_format) \
                .mode(write_mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.save(target_path)
            
            self.logger.info(f"Loaded {valid_count} records successfully")
            
            return valid_count
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise