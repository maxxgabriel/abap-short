"""
Data extraction module.

Handles extraction of raw sales data from source tables/files.
"""

from datetime import datetime
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLExtractor:
    """Extracts raw sales data from source."""

    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the extractor.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger

    def extract_data(self, from_date: datetime, to_date: datetime) -> DataFrame:
        """
        Extract sales data for the given date range.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction

        Returns:
            DataFrame containing extracted raw sales data
        """
        try:
            self.logger.log_message(
                step=ETLConstants.STEP_EXTRACT,
                status=ETLConstants.STATUS_SUCCESS,
                message=f"Starting extraction from {from_date.date()} to {to_date.date()}"
            )

            # Get source configuration
            source_config = self.config.get('source', {})
            source_type = source_config.get('type', 'csv')
            source_path = source_config.get('path', 'data/raw/sales_raw.csv')

            # Define schema
            schema = self._get_raw_sales_schema()

            # Extract based on source type
            if source_type == 'csv':
                df = self._extract_from_csv(source_path, schema)
            elif source_type == 'parquet':
                df = self._extract_from_parquet(source_path)
            elif source_type == 'jdbc':
                df = self._extract_from_jdbc(source_config)
            else:
                # For demonstration, generate sample data
                df = self._generate_sample_data(from_date, to_date)

            # Filter by date range
            df = df.filter(
                (df.trans_date >= from_date.date()) &
                (df.trans_date <= to_date.date()) &
                (df.status == ETLConstants.STATUS_NEW)
            )

            record_count = df.count()

            self.logger.log_message(
                step=ETLConstants.STEP_EXTRACT,
                status=ETLConstants.STATUS_SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )

            return df

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.STEP_EXTRACT,
                status=ETLConstants.STATUS_ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            raise

    def _get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.

        Returns:
            StructType schema definition
        """
        return StructType([
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

    def _extract_from_csv(self, path: str, schema: StructType) -> DataFrame:
        """Extract data from CSV file."""
        return self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(path)

    def _extract_from_parquet(self, path: str) -> DataFrame:
        """Extract data from Parquet file."""
        return self.spark.read.parquet(path)

    def _extract_from_jdbc(self, config: dict) -> DataFrame:
        """Extract data from JDBC source."""
        return self.spark.read \
            .format("jdbc") \
            .option("url", config.get('jdbc_url')) \
            .option("dbtable", config.get('table')) \
            .option("user", config.get('user')) \
            .option("password", config.get('password')) \
            .option("driver", config.get('driver', 'org.postgresql.Driver')) \
            .load()

    def _generate_sample_data(self, from_date: datetime,
                              to_date: datetime) -> DataFrame:
        """
        Generate sample data for demonstration.

        Args:
            from_date: Start date
            to_date: End date

        Returns:
            DataFrame with sample data
        """
        from decimal import Decimal

        sample_data = [
            ("T000001", from_date.date(), "CUST001", "PROD001", 10,
             Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", from_date.date(), "CUST002", "PROD002", 5,
             Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", from_date.date(), "CUST003", "PROD001", 20,
             Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", from_date.date(), "CUST001", "PROD003", 3,
             Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", from_date.date(), "CUST004", "PROD002", 15,
             Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
        ]

        schema = self._get_raw_sales_schema()
        return self.spark.createDataFrame(sample_data, schema)