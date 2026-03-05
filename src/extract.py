"""
Data extraction component for ETL system.
Converted from ABAP ZCL_ETL_EXTRACTOR.
"""

from datetime import date
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from src.logger import ETLLogger
from src.constants import ETLConstants
from src.schemas import ETLSchemas


class ETLExtractor:
    """Extracts raw sales data from source tables."""

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize extractor with logger and Spark session.

        Args:
            logger: ETL logger instance
            spark: Active SparkSession
        """
        self.logger = logger
        self.spark = spark

    def extract_data(
        self, from_date: date, to_date: date, source_table: str
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name or path

        Returns:
            DataFrame with raw sales data, or None if extraction fails
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}",
            )

            # Read from source table with date filter
            # In production, this would read from actual database/table
            df = (
                self.spark.read.format("parquet")
                .schema(ETLSchemas.raw_sales_schema())
                .load(source_table)
                .filter(
                    (col("trans_date") >= from_date)
                    & (col("trans_date") <= to_date)
                    & (col("status") == ETLConstants.Status.NEW)
                )
            )

            count = df.count()

            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                message=f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count,
            )

            return df

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.ERROR,
                message=f"Extraction failed: {str(e)}",
            )
            return None

    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing.
        Matches the demo data in ABAP extractor.

        Returns:
            DataFrame with sample sales data
        """
        from datetime import datetime

        sample_data = [
            {
                "trans_id": "T000001",
                "trans_date": datetime.now().date(),
                "customer_id": "CUST001",
                "product_id": "PROD001",
                "quantity": 10,
                "unit_price": 99.99,
                "currency": "USD",
                "sales_rep": "John Doe",
                "region": "NORTH",
                "status": "N",
                "created_at": datetime.now(),
                "created_by": "SYSTEM",
            },
            {
                "trans_id": "T000002",
                "trans_date": datetime.now().date(),
                "customer_id": "CUST002",
                "product_id": "PROD002",
                "quantity": 5,
                "unit_price": 149.99,
                "currency": "USD",
                "sales_rep": "Jane Smith",
                "region": "SOUTH",
                "status": "N",
                "created_at": datetime.now(),
                "created_by": "SYSTEM",
            },
            {
                "trans_id": "T000003",
                "trans_date": datetime.now().date(),
                "customer_id": "CUST003",
                "product_id": "PROD001",
                "quantity": 20,
                "unit_price": 99.99,
                "currency": "USD",
                "sales_rep": "John Doe",
                "region": "EAST",
                "status": "N",
                "created_at": datetime.now(),
                "created_by": "SYSTEM",
            },
            {
                "trans_id": "T000004",
                "trans_date": datetime.now().date(),
                "customer_id": "CUST001",
                "product_id": "PROD003",
                "quantity": 3,
                "unit_price": 299.99,
                "currency": "USD",
                "sales_rep": "Bob Wilson",
                "region": "WEST",
                "status": "N",
                "created_at": datetime.now(),
                "created_by": "SYSTEM",
            },
            {
                "trans_id": "T000005",
                "trans_date": datetime.now().date(),
                "customer_id": "CUST004",
                "product_id": "PROD002",
                "quantity": 15,
                "unit_price": 149.99,
                "currency": "USD",
                "sales_rep": "Jane Smith",
                "region": "SOUTH",
                "status": "N",
                "created_at": datetime.now(),
                "created_by": "SYSTEM",
            },
        ]

        return self.spark.createDataFrame(
            sample_data, schema=ETLSchemas.raw_sales_schema()
        )