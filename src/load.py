"""
Data loading component for Sales ETL system.
Converted from ABAP ZCL_ETL_LOADER.
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.schemas import ProcessSteps, SaleCategories, StatusCodes


class ETLLoader:
    """Loads transformed data into target analytics table."""

    def __init__(self, logger: ETLLogger):
        """
        Initialize loader.
        
        Args:
            logger: ETL logger instance
        """
        self.logger = logger

    def load_data(
        self,
        df_analytics: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append",
    ) -> bool:
        """
        Load analytics data to target.
        
        Args:
            df_analytics: DataFrame with analytics data
            target_path: Optional path to target location
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.log_message(
                step=ProcessSteps.LOAD,
                status=StatusCodes.SUCCESS,
                message="Starting data load",
            )

            record_count = df_analytics.count()

            # Validate records before loading
            df_valid = self._validate_records(df_analytics)
            valid_count = df_valid.count()
            error_count = record_count - valid_count

            if error_count > 0:
                self.logger.log_message(
                    step=ProcessSteps.LOAD,
                    status=StatusCodes.WARNING,
                    message=f"{error_count} invalid records will be skipped",
                )

            # Write to target
            if target_path:
                self._write_to_file(df_valid, target_path, mode)
            else:
                self._write_to_table(df_valid, mode)

            self.logger.log_message(
                step=ProcessSteps.LOAD,
                status=StatusCodes.SUCCESS,
                records_processed=record_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {record_count} records",
            )

            return True

        except Exception as e:
            self.logger.log_message(
                step=ProcessSteps.LOAD,
                status=StatusCodes.ERROR,
                message=f"Load failed: {str(e)}",
            )
            return False

    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        return df.filter(
            # Required fields must not be null
            F.col("analytics_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
            & (F.col("gross_amount") > 0)
            & F.col("currency").isNotNull()
            # Category must be valid
            & F.col("category").isin(
                SaleCategories.HIGH,
                SaleCategories.MEDIUM,
                SaleCategories.LOW,
            )
        )

    def _write_to_file(
        self,
        df: DataFrame,
        target_path: str,
        mode: str,
    ) -> None:
        """
        Write analytics data to file.
        
        Args:
            df: DataFrame to write
            target_path: Target file path
            mode: Write mode
        """
        # Write based on file format
        if target_path.endswith(".parquet"):
            df.write.mode(mode).parquet(target_path)
        elif target_path.endswith(".csv"):
            df.write.mode(mode).csv(target_path, header=True)
        elif target_path.endswith(".json"):
            df.write.mode(mode).json(target_path)
        else:
            # Default to parquet
            df.write.mode(mode).parquet(target_path)

    def _write_to_table(self, df: DataFrame, mode: str) -> None:
        """
        Write analytics data to database table.
        
        Args:
            df: DataFrame to write
            mode: Write mode
        """
        # In production, this would write to actual database
        # Example: df.write.jdbc(url, "zsales_analytics", mode, properties)
        
        # For now, write to temporary location
        df.write.mode(mode).saveAsTable("zsales_analytics")