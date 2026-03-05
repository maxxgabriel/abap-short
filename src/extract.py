"""
Data Extraction Module
Extracts raw sales data from source systems.
"""

from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import ExtractError


class SalesExtractor:
    """Extracts raw sales data from source table/files."""

    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the extractor.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger

    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data for the given date range.

        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame containing raw sales data

        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="I",
                message=f"Starting extraction from {from_date} to {to_date}"
            )

            # Get source configuration
            source_path = self.config.get("source_data_path")
            source_format = self.config.get("source_format", "parquet")

            # Define schema
            schema = self._get_raw_sales_schema()

            # Read data
            df = self.spark.read \
                .format(source_format) \
                .schema(schema) \
                .load(source_path)

            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )

            record_count = df_filtered.count()

            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )

            return df_filtered

        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(f"Failed to extract data: {str(e)}") from e

    def _get_raw_sales_schema(self) -> StructType:
        """
        Define the schema for raw sales data.

        Returns:
            StructType: Schema definition
        """
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False)
        ])