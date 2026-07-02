from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Tuple
from src.logger import ETLLogger
from src.constants import ProcessStep, ProcessStatus
from src.exceptions import LoadError, ValidationError


class DataLoader:
    """Loads transformed data into target analytics table"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        self.spark = spark
        self.logger = logger
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: str = None,
        mode: str = "append"
    ) -> Tuple[int, bool]:
        """
        Load analytics data to target
        
        Returns:
            Tuple of (records_loaded, success_flag)
        """
        try:
            self.logger.log_message(
                step=ProcessStep.LOAD,
                status=ProcessStatus.SUCCESS,
                message="Starting data load"
            )
            
            # Validate records
            validated_df = self._validate_records(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = validated_df.count()
            invalid_count = total_count - valid_count
            
            if invalid_count > 0:
                self.logger.log_message(
                    step=ProcessStep.LOAD,
                    status=ProcessStatus.WARNING,
                    message=f"Skipped {invalid_count} invalid records"
                )
            
            # Write to target
            if target_path:
                validated_df.write.mode(mode).parquet(target_path)
            else:
                # In-memory mode for testing
                validated_df.cache()
            
            self.logger.log_message(
                step=ProcessStep.LOAD,
                status=ProcessStatus.SUCCESS,
                message=f"Loaded {valid_count} of {total_count} records",
                records_processed=total_count,
                records_success=valid_count,
                records_error=invalid_count
            )
            
            return valid_count, True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.LOAD,
                status=ProcessStatus.ERROR,
                message=f"Load failed: {str(e)}"
            )
            raise LoadError(
                message=f"Failed to load data: {str(e)}",
                step=ProcessStep.LOAD
            )
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """Validate analytics records before loading"""
        
        return df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin("HIGH", "MEDIUM", "LOW"))
        )