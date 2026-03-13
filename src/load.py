"""
ETL Load Module
Loads transformed data with validation and logging
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.logger import ETLLogger, ETLStep, ETLStatus


class SalesDataLoader:
    """Load transformed analytics data to target"""

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize loader

        Args:
            logger: ETL logger instance
            spark: SparkSession instance
        """
        self.logger = logger
        self.spark = spark

    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading

        Args:
            df: DataFrame to validate

        Returns:
            DataFrame with validation flag
        """
        validated_df = df.withColumn(
            'is_valid',
            (F.col('analytics_id').isNotNull()) &
            (F.col('customer_id').isNotNull()) &
            (F.col('product_id').isNotNull()) &
            (F.col('gross_amount') > 0) &
            (F.col('currency').isNotNull()) &
            (F.col('category').isin(['HIGH', 'MEDIUM', 'LOW']))
        )

        return validated_df

    def load_data(
        self,
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        write_mode: str = 'append'
    ) -> bool:
        """
        Load analytics data to target

        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional target path for writing data
            write_mode: Write mode (append/overwrite)

        Returns:
            True if load successful, False otherwise

        Raises:
            Exception: If load fails
        """
        try:
            self.logger.log_info(
                ETLStep.LOAD,
                "Starting data load"
            )

            total_count = analytics_df.count()

            # Validate records
            validated_df = self.validate_record(analytics_df)

            # Count valid and invalid records
            valid_count = validated_df.filter(F.col('is_valid')).count()
            invalid_count = total_count - valid_count

            if invalid_count > 0:
                self.logger.log_warning(
                    ETLStep.LOAD,
                    f"Found {invalid_count} invalid records",
                    records_processed=total_count,
                    records_error=invalid_count
                )

                # Log sample invalid records for debugging
                invalid_records = validated_df.filter(~F.col('is_valid')).limit(5)
                for row in invalid_records.collect():
                    self.logger.log_warning(
                        ETLStep.LOAD,
                        f"Invalid record: {row.analytics_id}",
                        analytics_id=row.analytics_id,
                        customer_id=row.customer_id
                    )

            # Filter to valid records only
            valid_df = validated_df.filter(F.col('is_valid')).drop('is_valid')

            if target_path:
                # Write to target location
                valid_df.write.mode(write_mode).parquet(target_path)
                self.logger.log_success(
                    ETLStep.LOAD,
                    f"Data written to {target_path}",
                    records_processed=total_count,
                    records_success=valid_count,
                    records_error=invalid_count,
                    target_path=target_path
                )
            else:
                # For testing - just cache
                valid_df.cache()
                self.logger.log_info(
                    ETLStep.LOAD,
                    "Data cached (test mode)",
                    records_processed=total_count,
                    records_success=valid_count
                )

            self.logger.log_success(
                ETLStep.LOAD,
                f"Loaded {valid_count} of {total_count} records",
                records_processed=total_count,
                records_success=valid_count,
                records_error=invalid_count
            )

            return True

        except Exception as e:
            self.logger.log_error(
                ETLStep.LOAD,
                f"Load failed: {str(e)}",
                exception=e
            )
            return False