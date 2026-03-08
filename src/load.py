"""
PySpark Data Loading Module
Loads transformed analytics data to target with validation.
"""

from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.exceptions import LoadError


class DataLoader:
    """
    Handles loading of analytics data to target destination.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize data loader.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config

    def load_data(self, analytics_df: DataFrame, dry_run: bool = False) -> Tuple[int, int]:
        """
        Load analytics data to target destination.

        Args:
            analytics_df: Analytics DataFrame to load
            dry_run: If True, validate only without loading

        Returns:
            Tuple of (success_count, error_count)

        Raises:
            LoadError: If load fails
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )

            # Validate records
            validated_df = self._validate_records(analytics_df)

            # Count valid and invalid records
            total_count = validated_df.count()
            valid_df = validated_df.filter(F.col("is_valid") == True)
            invalid_df = validated_df.filter(F.col("is_valid") == False)

            success_count = valid_df.count()
            error_count = invalid_df.count()

            # Log validation results
            if error_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message=f'Found {error_count} invalid records',
                    records_error=error_count
                )

            if not dry_run and success_count > 0:
                # Remove validation column and load data
                output_df = valid_df.drop("is_valid")

                # Write to target (parquet for demonstration)
                target_path = self.config.get('target_path', 'data/analytics')
                output_df.write.mode("append").parquet(target_path)

                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    message=f'Loaded {success_count} records to {target_path}',
                    records_processed=total_count,
                    records_success=success_count,
                    records_error=error_count
                )
            else:
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    message=f'Dry run completed - {success_count} valid records',
                    records_processed=total_count,
                    records_success=success_count
                )

            return success_count, error_count

        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            raise LoadError(f"Data load failed: {str(e)}")

    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records.

        Args:
            df: DataFrame to validate

        Returns:
            DataFrame with is_valid column added
        """
        return df.withColumn(
            "is_valid",
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin("HIGH", "MEDIUM", "LOW"))
        )