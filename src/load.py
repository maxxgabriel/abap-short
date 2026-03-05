"""
Data loading module for Sales ETL System.
Loads transformed analytics data into target storage.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple, Optional
import logging

from src.logger import ETLLogger
from src.constants import ETLConstants


class DataLoader:
    """Loads transformed analytics data into target systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the data loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.constants = ETLConstants()
    
    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> Tuple[int, bool]:
        """
        Load analytics data to target storage.
        
        Args:
            analytics_df: DataFrame with analytics data
            target_path: Optional path to target storage
            mode: Write mode (append, overwrite, etc.)
        
        Returns:
            Tuple of (number of records loaded, success flag)
        """
        try:
            self.logger.log_message(
                step=self.constants.STEP_LOAD,
                status=self.constants.STATUS_INFO,
                message="Starting data load"
            )
            
            # Validate records before loading
            valid_df = self._validate_records(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            # Write to target
            if target_path:
                self._write_to_file(valid_df, target_path, mode)
            else:
                self._write_to_database(valid_df, mode)
            
            self.logger.log_message(
                step=self.constants.STEP_LOAD,
                status=self.constants.STATUS_SUCCESS,
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return valid_count, True
            
        except Exception as e:
            self.logger.log_message(
                step=self.constants.STEP_LOAD,
                status=self.constants.STATUS_ERROR,
                message=f"Load failed: {str(e)}"
            )
            logging.error(f"Load error: {str(e)}", exc_info=True)
            return 0, False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: Analytics DataFrame
        
        Returns:
            DataFrame with only valid records
        """
        # Filter out records with invalid data
        valid_df = df.filter(
            (df.analytics_id.isNotNull()) &
            (df.customer_id.isNotNull()) &
            (df.product_id.isNotNull()) &
            (df.gross_amount > 0) &
            (df.currency.isNotNull()) &
            (df.category.isin([
                self.constants.CATEGORY_HIGH,
                self.constants.CATEGORY_MEDIUM,
                self.constants.CATEGORY_LOW
            ]))
        )
        
        # Log validation results
        invalid_count = df.count() - valid_df.count()
        if invalid_count > 0:
            self.logger.log_message(
                step=self.constants.STEP_LOAD,
                status=self.constants.STATUS_WARNING,
                message=f"Filtered out {invalid_count} invalid records"
            )
        
        return valid_df
    
    def _write_to_file(self, df: DataFrame, target_path: str, mode: str) -> None:
        """
        Write data to file storage.
        
        Args:
            df: DataFrame to write
            target_path: Target file path
            mode: Write mode
        """
        if target_path.endswith('.parquet'):
            df.write.mode(mode).parquet(target_path)
        elif target_path.endswith('.json'):
            df.write.mode(mode).json(target_path)
        elif target_path.endswith('.csv'):
            df.write.mode(mode).option("header", "true").csv(target_path)
        else:
            raise ValueError(f"Unsupported file format: {target_path}")
    
    def _write_to_database(self, df: DataFrame, mode: str) -> None:
        """
        Write data to database.
        
        Args:
            df: DataFrame to write
            mode: Write mode
        """
        # In production, this would write to actual database
        # For now, log the action
        self.logger.log_message(
            step=self.constants.STEP_LOAD,
            status=self.constants.STATUS_INFO,
            message=f"Writing {df.count()} records to database (mode: {mode})"
        )