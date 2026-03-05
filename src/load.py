"""
ETL Loader Module - Loads transformed data into target analytics table
Converts ABAP ZCL_ETL_LOADER to PySpark Delta Lake operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Tuple

from src.logger import ETLLogger


class ETLLoader:
    """
    Loads transformed analytics data into Delta Lake (ABAP ZCL_ETL_LOADER equivalent).
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

    def load_data(self, analytics_df: DataFrame, target_path: str) -> bool:
        """
        Load analytics data to Delta Lake table.

        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Path to target Delta Lake table

        Returns:
            Success flag
        """
        try:
            self.logger.log_message(
                step=ETLLogger.STEP_LOAD,
                status=ETLLogger.STATUS_SUCCESS,
                message="Starting data load"
            )

            record_count = analytics_df.count()

            # Validate records before loading
            valid_df = analytics_df.filter(
                col("analytics_id").isNotNull() &
                col("customer_id").isNotNull() &
                col("product_id").isNotNull() &
                (col("gross_amount") > 0) &
                col("currency").isNotNull() &
                col("category").isin(["HIGH", "MEDIUM", "LOW"])
            )

            valid_count = valid_df.count()
            error_count = record_count - valid_count

            if error_count > 0:
                self.logger.log_message(
                    step=ETLLoader.STEP_LOAD,
                    status=ETLLogger.STATUS_WARNING,
                    message=f"Skipped {error_count} invalid records"
                )

            # Write to Delta Lake in append mode
            valid_df.write \
                .format("delta") \
                .mode("append") \
                .save(target_path)

            self.logger.log_message(
                step=ETLLogger.STEP_LOAD,
                status=ETLLogger.STATUS_SUCCESS,
                records_processed=record_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {record_count} records to {target_path}"
            )

            return True

        except Exception as e:
            self.logger.log_message(
                step=ETLLogger.STEP_LOAD,
                status=ETLLogger.STATUS_ERROR,
                message=f"Load failed: {str(e)}"
            )
            return False

    def validate_record(self, row: dict) -> bool:
        """
        Validate a single record (similar to ABAP validate_record).

        Args:
            row: Dictionary representing a record

        Returns:
            Validation result
        """
        required_fields = [
            'analytics_id', 'customer_id', 'product_id', 'currency', 'category'
        ]

        # Check required fields
        for field in required_fields:
            if not row.get(field):
                return False

        # Check gross amount > 0
        if row.get('gross_amount', 0) <= 0:
            return False

        # Check category values
        if row.get('category') not in ['HIGH', 'MEDIUM', 'LOW']:
            return False

        return True