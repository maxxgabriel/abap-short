"""
Data Loading Module
Loads transformed analytics data to target destination.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.logger import ETLLogger
from src.exceptions import LoadError


class SalesLoader:
    """Loads analytics data to target table/storage."""

    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the loader.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger

    def load_data(self, analytics_data: DataFrame) -> bool:
        """
        Load analytics data to target destination.

        Args:
            analytics_data: Analytics DataFrame to load

        Returns:
            bool: True if successful

        Raises:
            LoadError: If load fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="I",
                message="Starting data load"
            )

            # Validate records before loading
            df_valid = self._validate_records(analytics_data)
            
            valid_count = df_valid.count()
            total_count = analytics_data.count()
            error_count = total_count - valid_count

            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )

            # Get target configuration
            target_path = self.config.get("target_data_path")
            target_format = self.config.get("target_format", "parquet")
            write_mode = self.config.get("write_mode", "append")

            # Write to target
            df_valid.write \
                .format(target_format) \
                .mode(write_mode) \
                .save(target_path)

            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )

            # Update source status (if configured)
            if self.config.get("update_source_status", False):
                self._update_source_status(analytics_data)

            return True

        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise LoadError(f"Failed to load data: {str(e)}") from e

    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with only valid records
        """
        # Validate required fields are not null
        df_valid = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0)
        )

        # Validate currency is not empty
        df_valid = df_valid.filter(col("currency").isNotNull())

        # Validate category values
        df_valid = df_valid.filter(
            col("category").isin(["HIGH", "MEDIUM", "LOW"])
        )

        return df_valid

    def _update_source_status(self, analytics_data: DataFrame):
        """
        Update source records to mark them as processed.

        Args:
            analytics_data: Analytics DataFrame with trans_ids
        """
        try:
            source_path = self.config.get("source_data_path")
            
            # Read source data
            source_df = self.spark.read.parquet(source_path)
            
            # Get list of processed transaction IDs
            processed_ids = analytics_data.select("trans_id").distinct()
            
            # Update status to 'P' (Processed)
            updated_df = source_df.join(
                processed_ids,
                on="trans_id",
                how="left"
            ).withColumn(
                "status",
                when(col("trans_id").isNotNull(), "P").otherwise(col("status"))
            )
            
            # Overwrite source (use with caution in production)
            updated_df.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(source_path)
            
            self.logger.log_message(
                step="LOAD",
                status="I",
                message="Source status updated successfully"
            )
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Failed to update source status: {str(e)}"
            )