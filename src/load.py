"""
PySpark Data Loader Module
Loads transformed analytics data into target with validation.
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.logger import ETLLogger


class SalesDataLoader:
    """
    Loads transformed analytics data into target storage.
    Performs validation before loading.
    """
    
    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize loader with logger and configuration.
        
        Args:
            logger: ETLLogger instance for tracking operations
            config: Configuration dictionary with load settings
        """
        self.logger = logger
        self.config = config
    
    def load_data(
        self, 
        analytics_df: DataFrame, 
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target storage.
        
        Args:
            analytics_df: DataFrame containing analytics data
            target_path: Optional path to target. If None, uses config default.
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            # Validate records before loading
            validated_df = self._validate_records(analytics_df)
            
            input_count = analytics_df.count()
            valid_count = validated_df.count()
            error_count = input_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Determine target path
            if not target_path:
                target_path = self.config['paths']['target']
            
            # Write to target
            validated_df.write.mode(mode).parquet(target_path)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=input_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {input_count} records to {target_path}"
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=error_msg
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        # Validate required fields are not null
        validated_df = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            col("gross_amount").isNotNull()
        )
        
        # Validate gross_amount is positive
        validated_df = validated_df.filter(col("gross_amount") > 0)
        
        # Validate currency is not empty
        validated_df = validated_df.filter(
            (col("currency").isNotNull()) & 
            (col("currency") != "")
        )
        
        # Validate category is valid
        validated_df = validated_df.filter(
            col("category").isin(["HIGH", "MEDIUM", "LOW"])
        )
        
        return validated_df