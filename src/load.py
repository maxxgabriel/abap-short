"""
Data loading module for Sales ETL pipeline.
Loads transformed analytics data to target destination.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Optional
import logging


class SalesDataLoader:
    """Loads transformed analytics data to target systems."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target destination.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional override for target path
            mode: Write mode (append, overwrite, error, ignore)
            
        Returns:
            True if load successful, False otherwise
        """
        self.logger.info("Starting data load")
        
        try:
            # Validate data before loading
            if not self._validate_records(analytics_df):
                self.logger.error("Data validation failed, aborting load")
                return False
            
            record_count = analytics_df.count()
            self.logger.info(f"Loading {record_count} records")
            
            # Get target path from config or parameter
            path = target_path or self.config.get("target_data_path")
            
            if not path:
                self.logger.warning(
                    "No target path specified, displaying sample data"
                )
                analytics_df.show(10, truncate=False)
                return True
            
            # Partition by date for better query performance
            partition_cols = self.config.get("partition_columns", ["trans_date"])
            
            # Write data
            analytics_df.write \
                .mode(mode) \
                .partitionBy(*partition_cols) \
                .parquet(path)
            
            self.logger.info(
                f"Successfully loaded {record_count} records to {path}"
            )
            
            # Update source status (if enabled)
            if self.config.get("update_source_status", False):
                self._update_source_status(analytics_df)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return False
    
    def _validate_records(self, df: DataFrame) -> bool:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if all validations pass
        """
        # Check for required fields
        required_fields = [
            "analytics_id", "customer_id", "product_id", 
            "gross_amount", "currency", "category"
        ]
        
        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                self.logger.error(
                    f"Validation failed: {null_count} null values in {field}"
                )
                return False
        
        # Check for invalid amounts
        invalid_amounts = df.filter(col("gross_amount") <= 0).count()
        if invalid_amounts > 0:
            self.logger.error(
                f"Validation failed: {invalid_amounts} records with invalid amounts"
            )
            return False
        
        # Check for valid categories
        valid_categories = ["HIGH", "MEDIUM", "LOW"]
        invalid_categories = df.filter(
            ~col("category").isin(valid_categories)
        ).count()
        
        if invalid_categories > 0:
            self.logger.error(
                f"Validation failed: {invalid_categories} records with invalid category"
            )
            return False
        
        self.logger.info("Record validation passed")
        return True
    
    def _update_source_status(self, analytics_df: DataFrame):
        """
        Update source table status (placeholder for actual implementation).
        
        Args:
            analytics_df: Analytics DataFrame with processed records
        """
        # In production, this would update the source table status
        # from 'N' (new) to 'P' (processed)
        trans_ids = [
            row.trans_id 
            for row in analytics_df.select("trans_id").distinct().collect()
        ]
        
        self.logger.info(
            f"Would update status for {len(trans_ids)} source records"
        )
    
    def write_to_database(
        self,
        analytics_df: DataFrame,
        jdbc_url: str,
        table_name: str,
        properties: dict
    ) -> bool:
        """
        Write data to database via JDBC.
        
        Args:
            analytics_df: Analytics DataFrame
            jdbc_url: JDBC connection URL
            table_name: Target table name
            properties: JDBC connection properties
            
        Returns:
            True if successful
        """
        self.logger.info(f"Loading data to database table: {table_name}")
        
        try:
            analytics_df.write \
                .jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode="append",
                    properties=properties
                )
            
            self.logger.info("Successfully loaded data to database")
            return True
            
        except Exception as e:
            self.logger.error(f"Database load failed: {str(e)}")
            return False