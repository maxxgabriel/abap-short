"""
Sales Data Loader Module
Loads transformed analytics data into target tables.
"""

from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.exceptions import LoadError


class SalesLoader:
    """
    Loads transformed analytics data into target tables.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger instance
        config: Configuration dictionary
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the sales loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(self, analytics_data: DataFrame) -> bool:
        """
        Load analytics data into target table.
        
        Args:
            analytics_data: Transformed analytics DataFrame
        
        Returns:
            bool: True if load successful, False otherwise
        
        Raises:
            LoadError: If load operation fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            # Validate records before loading
            validated_data = self._validate_records(analytics_data)
            
            total_count = analytics_data.count()
            success_count = validated_data.count()
            error_count = total_count - success_count
            
            # In production, write to target table
            # validated_data.write \
            #     .format(self.config.get("target_format", "parquet")) \
            #     .mode(self.config.get("write_mode", "append")) \
            #     .save(self.config.get("target_path"))
            
            # For demonstration, show the data
            if self.config.get("show_output", True):
                print("Sample of loaded records:")
                validated_data.show(5, truncate=False)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Loaded {success_count} of {total_count} records"
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=error_msg
            )
            raise LoadError(
                error_text=error_msg,
                error_step="LOAD"
            ) from e
    
    def _validate_records(self, analytics_data: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            analytics_data: Analytics DataFrame to validate
        
        Returns:
            DataFrame with only valid records
        """
        # Filter out invalid records
        valid_data = analytics_data.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        # Log validation results
        invalid_count = analytics_data.count() - valid_data.count()
        if invalid_count > 0:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Filtered out {invalid_count} invalid records"
            )
        
        return valid_data