"""
Data loading component.
Migrated from ABAP ZCL_ETL_LOADER class.
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.constants import ProcessStep, StatusCode, SaleCategory
from src.logger import ETLLogger


class ETLLoader:
    """
    Loads transformed data into target analytics table.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: str,
        mode: str = "append"
    ) -> bool:
        """
        Load transformed data to target.
        
        Args:
            analytics_df: DataFrame with analytics data
            target_path: Target path for data storage
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Success flag
        """
        try:
            self.logger.log_message(
                step=ProcessStep.LOAD.value,
                status=StatusCode.INFO.value,
                message="Starting data load"
            )
            
            # Validate records
            valid_df = self._validate_records(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step=ProcessStep.LOAD.value,
                    status=StatusCode.WARNING.value,
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Write to target
            valid_df.write.mode(mode).parquet(target_path)
            
            self.logger.log_message(
                step=ProcessStep.LOAD.value,
                status=StatusCode.SUCCESS.value,
                message=f"Loaded {valid_count} of {total_count} records",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.LOAD.value,
                status=StatusCode.ERROR.value,
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        # Validate required fields
        valid_df = df.filter(
            F.col("analytics_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("product_id").isNotNull() &
            (F.col("gross_amount") > 0) &
            F.col("currency").isNotNull() &
            F.col("category").isin([c.value for c in SaleCategory])
        )
        
        return valid_df
    
    def update_source_status(
        self,
        source_path: str,
        processed_ids: list,
        new_status: str = StatusCode.PROCESSED.value
    ) -> bool:
        """
        Update status of processed records in source.
        
        Args:
            source_path: Source data path
            processed_ids: List of processed transaction IDs
            new_status: New status value
            
        Returns:
            Success flag
        """
        try:
            # Read source data
            source_df = self.spark.read.parquet(source_path)
            
            # Update status for processed records
            updated_df = source_df.withColumn(
                "status",
                F.when(
                    F.col("trans_id").isin(processed_ids),
                    F.lit(new_status)
                ).otherwise(F.col("status"))
            )
            
            # Write back
            updated_df.write.mode("overwrite").parquet(source_path)
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.LOAD.value,
                status=StatusCode.WARNING.value,
                message=f"Failed to update source status: {str(e)}"
            )
            return False