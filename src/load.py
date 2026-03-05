"""
Data loading module for ETL pipeline.
Loads transformed analytics data into target storage.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Optional, Dict
import logging


class DataLoader:
    """Handles loading of analytics data into target storage."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the data loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load analytics data into target storage.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            True if load succeeds, False otherwise
        """
        try:
            self.logger.info("Starting data load")
            
            # Get target configuration
            target_path = self.config.get('target_path', 'data/analytics/sales')
            target_format = self.config.get('target_format', 'parquet')
            write_mode = self.config.get('write_mode', 'append')
            partition_by = self.config.get('partition_by', ['trans_date'])
            
            # Validate data before loading
            if not self._validate_before_load(analytics_df):
                raise ValueError("Data validation failed before load")
            
            # Write to target
            writer = analytics_df.write \
                .format(target_format) \
                .mode(write_mode)
            
            # Add partitioning if configured
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.save(target_path)
            
            record_count = analytics_df.count()
            self.logger.info(f"Loaded {record_count} records successfully to {target_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def _validate_before_load(self, df: DataFrame) -> bool:
        """
        Validate data before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if validation passes
        """
        try:
            # Check for required columns
            required_cols = ["analytics_id", "customer_id", "product_id", "gross_amount"]
            missing_cols = [c for c in required_cols if c not in df.columns]
            
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                return False
            
            # Check for empty DataFrame
            if df.rdd.isEmpty():
                self.logger.warning("DataFrame is empty")
                return False
            
            # Check for invalid categories
            valid_categories = ['HIGH', 'MEDIUM', 'LOW']
            invalid_category_count = df.filter(
                ~col("category").isin(valid_categories)
            ).count()
            
            if invalid_category_count > 0:
                self.logger.error(f"Found {invalid_category_count} records with invalid category")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            return False
    
    def update_source_status(self, trans_ids: list) -> bool:
        """
        Update status of processed records in source.
        
        Args:
            trans_ids: List of transaction IDs to update
            
        Returns:
            True if update succeeds
        """
        try:
            self.logger.info(f"Updating status for {len(trans_ids)} records")
            
            source_path = self.config.get('source_path', 'data/raw/sales')
            
            # Read source data
            df = self.spark.read.parquet(source_path)
            
            # Update status for processed records
            df_updated = df.withColumn(
                "status",
                col("status").when(col("trans_id").isin(trans_ids), "P").otherwise(col("status"))
            )
            
            # Write back (in production, use delta/merge)
            df_updated.write \
                .mode("overwrite") \
                .parquet(source_path + "_temp")
            
            self.logger.info("Source status updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Status update failed: {str(e)}")
            return False
    
    def get_load_statistics(self, analytics_df: DataFrame) -> Dict[str, int]:
        """
        Calculate statistics for loaded data.
        
        Args:
            analytics_df: DataFrame containing loaded data
            
        Returns:
            Dictionary with statistics
        """
        try:
            total_records = analytics_df.count()
            
            category_counts = analytics_df.groupBy("category").count().collect()
            category_stats = {row["category"]: row["count"] for row in category_counts}
            
            stats = {
                "total_records": total_records,
                "high_value_sales": category_stats.get("HIGH", 0),
                "medium_value_sales": category_stats.get("MEDIUM", 0),
                "low_value_sales": category_stats.get("LOW", 0)
            }
            
            self.logger.info(f"Load statistics: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to calculate statistics: {str(e)}")
            return {}