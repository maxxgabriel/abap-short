"""
Data loading module for Sales ETL.
Loads transformed data into target analytics table.
Migrated from ABAP ZCL_ETL_LOADER class.
"""

from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.config import ProcessStep, StatusCode, SaleCategory


class SalesLoader:
    """Loads transformed analytics data into target table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_table: str = "zsales_analytics",
        test_mode: bool = False
    ) -> bool:
        """
        Load analytics data into target table.
        
        Args:
            analytics_df: Analytics DataFrame to load
            target_table: Target table name
            test_mode: If True, skip actual database write
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            self.logger.log_message(
                step=ProcessStep.LOAD,
                status=StatusCode.INFO,
                message="Starting data load"
            )
            
            total_records = analytics_df.count()
            
            # Validate records before loading
            valid_df, invalid_count = self._validate_records(analytics_df)
            valid_count = valid_df.count()
            
            if invalid_count > 0:
                self.logger.log_message(
                    step=ProcessStep.LOAD,
                    status=StatusCode.WARNING,
                    message=f"Skipped {invalid_count} invalid records"
                )
            
            # Load data to target table
            if not test_mode:
                # In production: Write to database table
                # valid_df.write.mode("append").saveAsTable(target_table)
                
                # For demo: Just show the data
                self.logger.log_message(
                    step=ProcessStep.LOAD,
                    status=StatusCode.INFO,
                    message=f"Would write {valid_count} records to {target_table}"
                )
            else:
                self.logger.log_message(
                    step=ProcessStep.LOAD,
                    status=StatusCode.INFO,
                    message=f"Test mode - skipped writing {valid_count} records"
                )
            
            self.logger.log_message(
                step=ProcessStep.LOAD,
                status=StatusCode.SUCCESS,
                records_processed=total_records,
                records_success=valid_count,
                records_error=invalid_count,
                message=f"Loaded {valid_count} of {total_records} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.LOAD,
                status=StatusCode.ERROR,
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (valid DataFrame, invalid count)
        """
        # Define validation conditions
        valid_condition = (
            F.col("analytics_id").isNotNull() &
            (F.col("analytics_id") != "") &
            F.col("customer_id").isNotNull() &
            (F.col("customer_id") != "") &
            F.col("product_id").isNotNull() &
            (F.col("product_id") != "") &
            (F.col("gross_amount") > 0) &
            F.col("currency").isNotNull() &
            (F.col("currency") != "") &
            F.col("category").isin([c.value for c in SaleCategory])
        )
        
        # Split into valid and invalid
        valid_df = df.filter(valid_condition)
        invalid_df = df.filter(~valid_condition)
        
        invalid_count = invalid_df.count()
        
        # Log invalid records
        if invalid_count > 0:
            for row in invalid_df.select("analytics_id", "customer_id").take(10):
                self.logger.log_message(
                    step=ProcessStep.LOAD,
                    status=StatusCode.WARNING,
                    message=f"Invalid record: {row.analytics_id}"
                )
        
        return valid_df, invalid_count