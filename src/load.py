"""
Load module for Sales ETL pipeline.
Handles data loading to target systems.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple
import logging

from src.logger import ETLLogger


class SalesLoader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Filtered DataFrame with valid records
        """
        valid_df = df.filter(
            (df.analytics_id.isNotNull()) &
            (df.customer_id.isNotNull()) &
            (df.product_id.isNotNull()) &
            (df.gross_amount > 0) &
            (df.currency.isNotNull()) &
            (df.category.isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        return valid_df
    
    def load_data(self, analytics_df: DataFrame, target_path: str) -> Tuple[int, int, bool]:
        """
        Load analytics data to target.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Target path for analytics data
            
        Returns:
            Tuple of (success_count, error_count, success_flag)
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            total_count = analytics_df.count()
            
            # Validate records
            valid_df = self.validate_record(analytics_df)
            success_count = valid_df.count()
            error_count = total_count - success_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"{error_count} invalid records skipped"
                )
            
            # Write to target (append mode for incremental loads)
            valid_df.write \
                .mode(self.config["load"]["write_mode"]) \
                .partitionBy("trans_date") \
                .parquet(target_path)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Loaded {success_count} of {total_count} records"
            )
            
            return success_count, error_count, True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            logging.error(f"Load error: {str(e)}", exc_info=True)
            return 0, total_count if 'total_count' in locals() else 0, False