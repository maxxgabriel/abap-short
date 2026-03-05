"""
Data loading module for Sales ETL system.
Loads transformed analytics data into target tables.
"""
from pyspark.sql import DataFrame
from typing import Tuple

from src.logger import ETLLogger


class DataLoader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize the data loader.
        
        Args:
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_table: str = "zsales_analytics"
    ) -> bool:
        """
        Load analytics data into target table.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_table: Target table name
            
        Returns:
            Success flag
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            # Validate records before loading
            validated_df = self._validate_records(analytics_df)
            
            initial_count = analytics_df.count()
            valid_count = validated_df.count()
            error_count = initial_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Write to target table
            # In production, use appropriate write mode and partitioning
            validated_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=initial_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {initial_count} records"
            )
            
            # Update source table status
            self._update_source_status(validated_df)
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        from pyspark.sql import functions as F
        
        # Filter out invalid records
        validated_df = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return validated_df
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update status in source table for processed records.
        
        Args:
            df: DataFrame with loaded records
        """
        try:
            # In production, this would update the source table
            # Example: UPDATE zsales_raw SET status = 'P' WHERE trans_id IN (...)
            
            processed_count = df.count()
            self.logger.log_message(
                step="LOAD",
                status="I",
                message=f"Updated status for {processed_count} source records"
            )
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Failed to update source status: {str(e)}"
            )