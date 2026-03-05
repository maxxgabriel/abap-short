"""
Load Module
Handles loading of transformed analytics data into target storage.
"""

from datetime import datetime
from typing import Optional
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp

from src.logger import ETLLogger
from src.exceptions import LoadError


class SalesLoader:
    """
    Loads transformed analytics data into target storage.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the loader.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.stats = {
            "records_loaded": 0,
            "records_error": 0
        }

    def load_data(self, analytics_data: DataFrame) -> None:
        """
        Load analytics data to target storage.

        Args:
            analytics_data: Analytics DataFrame to load

        Raises:
            LoadError: If load operation fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )

            total_records = analytics_data.count()

            # Validate before loading
            self._validate_before_load(analytics_data)

            # Get target configuration
            target_config = self.config.get("target", {})
            target_type = target_config.get("type", "parquet")
            target_path = target_config.get("path", "data/analytics/sales_analytics")

            # Add load timestamp
            df_with_timestamp = analytics_data.withColumn(
                "loaded_at",
                current_timestamp()
            )

            # Load data based on target type
            if target_type == "parquet":
                self._load_to_parquet(df_with_timestamp, target_path, target_config)
            elif target_type == "delta":
                self._load_to_delta(df_with_timestamp, target_path, target_config)
            elif target_type == "jdbc":
                self._load_to_jdbc(df_with_timestamp, target_config)
            else:
                raise LoadError(f"Unsupported target type: {target_type}")

            self.stats["records_loaded"] = total_records

            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_records,
                records_success=total_records,
                message=f"Loaded {total_records} records successfully"
            )

        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            logging.error(f"Load error: {str(e)}", exc_info=True)
            raise LoadError(f"Failed to load data: {str(e)}") from e

    def _validate_before_load(self, df: DataFrame) -> None:
        """
        Validate data before loading.

        Args:
            df: DataFrame to validate

        Raises:
            LoadError: If validation fails
        """
        # Check for null values in required fields
        required_fields = ["analytics_id", "customer_id", "product_id", "gross_amount"]

        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                raise LoadError(f"Found {null_count} null values in required field: {field}")

        # Validate currency codes
        invalid_currency = df.filter(col("currency").isNull()).count()
        if invalid_currency > 0:
            raise LoadError(f"Found {invalid_currency} records with invalid currency")

        # Validate categories
        invalid_category = df.filter(
            ~col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()
        if invalid_category > 0:
            raise LoadError(f"Found {invalid_category} records with invalid category")

    def _load_to_parquet(
        self, df: DataFrame, target_path: str, config: dict
    ) -> None:
        """
        Load data to Parquet format.

        Args:
            df: DataFrame to load
            target_path: Target path for Parquet files
            config: Target configuration
        """
        mode = config.get("mode", "append")
        partition_by = config.get("partition_by", ["trans_date"])

        df.write.mode(mode).partitionBy(*partition_by).parquet(target_path)

    def _load_to_delta(
        self, df: DataFrame, target_path: str, config: dict
    ) -> None:
        """
        Load data to Delta Lake format.

        Args:
            df: DataFrame to load
            target_path: Target path for Delta table
            config: Target configuration
        """
        mode = config.get("mode", "append")
        partition_by = config.get("partition_by", ["trans_date"])

        df.write.format("delta").mode(mode).partitionBy(*partition_by).save(target_path)

    def _load_to_jdbc(self, df: DataFrame, config: dict) -> None:
        """
        Load data to JDBC target.

        Args:
            df: DataFrame to load
            config: Target configuration including JDBC connection details
        """
        jdbc_url = config.get("jdbc_url")
        table_name = config.get("table", "zsales_analytics")
        mode = config.get("mode", "append")

        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=config.get("properties", {})
        )

    def get_stats(self) -> dict:
        """Get load statistics."""
        return self.stats.copy()