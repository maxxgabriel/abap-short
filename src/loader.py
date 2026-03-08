"""
Loader Module
Handles loading transformed data into target systems.
"""
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.constants import ETLStep, ETLStatus, SaleCategory
from src.exceptions import LoadError, ValidationError


class ETLLoader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize loader.
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_table: str = "zsales_analytics"
    ) -> Tuple[int, bool]:
        """
        Load transformed data into target table.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_table: Target table name
            
        Returns:
            Tuple of (records_loaded, success_flag)
            
        Raises:
            LoadError: If loading fails
        """
        try:
            self.logger.log_message(
                step=ETLStep.LOAD,
                status=ETLStatus.SUCCESS,
                message="Starting data load"
            )
            
            # Validate records before loading
            valid_df, invalid_count = self._validate_records(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = valid_df.count()
            
            if invalid_count > 0:
                self.logger.log_message(
                    step=ETLStep.LOAD,
                    status=ETLStatus.WARNING,
                    message=f"Skipped {invalid_count} invalid records"
                )
            
            # Load to target (in production, use actual table writes)
            # valid_df.write.mode("append").saveAsTable(target_table)
            
            # For demo, just show the data
            self.logger.log_message(
                step=ETLStep.LOAD,
                status=ETLStatus.INFO,
                message="Sample records to be loaded:"
            )
            valid_df.show(5, truncate=False)
            
            self.logger.log_message(
                step=ETLStep.LOAD,
                status=ETLStatus.SUCCESS,
                message=f"Loaded {valid_count} of {total_count} records",
                records_processed=total_count,
                records_success=valid_count,
                records_error=invalid_count
            )
            
            return valid_count, True
            
        except Exception as e:
            self.logger.log_message(
                step=ETLStep.LOAD,
                status=ETLStatus.ERROR,
                message=f"Load failed: {str(e)}"
            )
            raise LoadError(f"Failed to load data: {str(e)}", previous=e)
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (valid DataFrame, invalid count)
        """
        initial_count = df.count()
        
        # Validate required fields
        valid_df = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin([SaleCategory.HIGH, SaleCategory.MEDIUM, SaleCategory.LOW]))
        )
        
        valid_count = valid_df.count()
        invalid_count = initial_count - valid_count
        
        return valid_df, invalid_count