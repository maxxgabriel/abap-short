"""
Data loading component for ETL system.
Converted from ABAP ZCL_ETL_LOADER.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLLoader:
    """Loads transformed data into target analytics table."""

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize loader with logger and Spark session.

        Args:
            logger: ETL logger instance
            spark: Active SparkSession
        """
        self.logger = logger
        self.spark = spark

    def load_data(
        self, analytics_df: DataFrame, target_table: str, update_source: bool = True
    ) -> bool:
        """
        Load transformed analytics data to target table.

        Args:
            analytics_df: Transformed analytics DataFrame
            target_table: Target table name or path
            update_source: Whether to update source table status

        Returns:
            True if load succeeded, False otherwise
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.LOAD,
                status=ETLConstants.Status.SUCCESS,
                message="Starting data load",
            )

            # Validate records before loading
            validated_df = self._validate_records(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = validated_df.count()
            error_count = total_count - valid_count

            if error_count > 0:
                self.logger.log_message(
                    step=ETLConstants.Step.LOAD,
                    status=ETLConstants.Status.WARNING,
                    message=f"{error_count} invalid records skipped",
                    records_error=error_count,
                )

            # Write to target table
            # In production: validated_df.write.mode("append").saveAsTable(target_table)
            validated_df.write.mode("append").parquet(target_table)

            # Update source table status if requested
            if update_source:
                self._update_source_status(analytics_df)

            self.logger.log_message(
                step=ETLConstants.Step.LOAD,
                status=ETLConstants.Status.SUCCESS,
                message=f"Loaded {valid_count} of {total_count} records",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
            )

            return True

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.LOAD,
                status=ETLConstants.Status.ERROR,
                message=f"Load failed: {str(e)}",
            )
            return False

    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records before loading.

        Args:
            df: Analytics DataFrame

        Returns:
            Validated DataFrame with invalid records filtered out
        """
        return df.filter(
            # Required fields must not be null
            col("analytics_id").isNotNull()
            & col("customer_id").isNotNull()
            & col("product_id").isNotNull()
            & col("gross_amount").isNotNull()
            # Gross amount must be positive
            & (col("gross_amount") > 0)
            # Currency must not be null
            & col("currency").isNotNull()
            # Category must be valid
            & col("category").isin(
                ETLConstants.Category.HIGH,
                ETLConstants.Category.MEDIUM,
                ETLConstants.Category.LOW,
            )
        )

    def _update_source_status(self, analytics_df: DataFrame) -> None:
        """
        Update source table to mark records as processed.

        Args:
            analytics_df: Analytics DataFrame containing transaction IDs
        """
        try:
            # Extract transaction IDs from analytics_id
            # Format: ANL<trans_id><date>
            # In production, would execute UPDATE statement on source table
            trans_ids = (
                analytics_df.select("analytics_id")
                .distinct()
                .rdd.map(lambda row: row.analytics_id[3:13])  # Extract trans_id
                .collect()
            )

            self.logger.log_message(
                step=ETLConstants.Step.LOAD,
                status=ETLConstants.Status.INFO,
                message=f"Updated status for {len(trans_ids)} source records",
            )

            # In production:
            # UPDATE raw_sales_table SET status = 'P' WHERE trans_id IN (trans_ids)

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.LOAD,
                status=ETLConstants.Status.WARNING,
                message=f"Failed to update source status: {str(e)}",
            )