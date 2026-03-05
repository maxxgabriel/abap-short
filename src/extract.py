"""
Extract Module
Handles extraction of raw sales data from source tables/files.
"""

from datetime import datetime
from typing import Optional
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.exceptions import ExtractError


class SalesExtractor:
    """
    Extracts raw sales data from source.
    """

    # Schema for raw sales data
    RAW_SALES_SCHEMA = StructType([
        StructField("trans_id", StringType(), False),
        StructField("trans_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), False)
    ])

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the extractor.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.stats = {"records_extracted": 0}

    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data for the specified date range.

        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with raw sales data

        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )

            # Get source configuration
            source_config = self.config.get("source", {})
            source_type = source_config.get("type", "parquet")
            source_path = source_config.get("path", "data/raw/sales_raw")

            # Extract data based on source type
            if source_type == "parquet":
                raw_data = self._extract_from_parquet(source_path, from_date, to_date)
            elif source_type == "csv":
                raw_data = self._extract_from_csv(source_path, from_date, to_date)
            elif source_type == "jdbc":
                raw_data = self._extract_from_jdbc(source_config, from_date, to_date)
            else:
                raise ExtractError(f"Unsupported source type: {source_type}")

            # Validate extracted data
            self._validate_extracted_data(raw_data)

            record_count = raw_data.count()
            self.stats["records_extracted"] = record_count

            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )

            return raw_data

        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            logging.error(f"Extraction error: {str(e)}", exc_info=True)
            raise ExtractError(f"Failed to extract data: {str(e)}") from e

    def _extract_from_parquet(
        self, source_path: str, from_date: str, to_date: str
    ) -> DataFrame:
        """Extract data from Parquet files."""
        df = self.spark.read.parquet(source_path)

        # Filter by date range and status
        filtered_df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date) &
            (df.status == 'N')
        )

        return filtered_df

    def _extract_from_csv(
        self, source_path: str, from_date: str, to_date: str
    ) -> DataFrame:
        """Extract data from CSV files."""
        df = self.spark.read.csv(
            source_path,
            schema=self.RAW_SALES_SCHEMA,
            header=True
        )

        # Filter by date range and status
        filtered_df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date) &
            (df.status == 'N')
        )

        return filtered_df

    def _extract_from_jdbc(
        self, source_config: dict, from_date: str, to_date: str
    ) -> DataFrame:
        """Extract data from JDBC source."""
        jdbc_url = source_config.get("jdbc_url")
        table_name = source_config.get("table", "zsales_raw")

        query = f"""
            (SELECT * FROM {table_name}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') as sales_data
        """

        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=source_config.get("properties", {})
        )

        return df

    def _validate_extracted_data(self, df: DataFrame) -> None:
        """
        Validate extracted data.

        Args:
            df: Extracted DataFrame

        Raises:
            ExtractError: If validation fails
        """
        # Check if DataFrame is empty
        if df.isEmpty():
            raise ExtractError("No data extracted from source")

        # Validate schema
        required_columns = [field.name for field in self.RAW_SALES_SCHEMA.fields]
        actual_columns = df.columns

        missing_columns = set(required_columns) - set(actual_columns)
        if missing_columns:
            raise ExtractError(f"Missing required columns: {missing_columns}")

        # Check for null values in critical fields
        null_counts = df.select([
            df[col].isNull().cast("int").alias(col)
            for col in ["trans_id", "trans_date", "customer_id", "product_id"]
        ]).groupBy().sum().collect()[0].asDict()

        critical_nulls = {k: v for k, v in null_counts.items() if v > 0}
        if critical_nulls:
            raise ExtractError(f"Null values found in critical fields: {critical_nulls}")

    def get_stats(self) -> dict:
        """Get extraction statistics."""
        return self.stats.copy()