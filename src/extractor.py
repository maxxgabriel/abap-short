"""
ETL Extractor Module
Extracts raw sales data from source
Migrated from ZCL_ETL_EXTRACTOR ABAP class
"""

from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger


class ETLExtractor:
    """
    Data extraction component for ETL pipeline.
    Reads raw sales data from source and returns as Spark DataFrame.
    """

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize extractor with logger and Spark session.
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark

    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        Migrates ABAP ty_raw_sales structure to PySpark StructType.
        
        Returns:
            StructType schema for raw sales data
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

    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for date range.
        Migrates ABAP SELECT statement to PySpark DataFrame operations.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            
        Returns:
            DataFrame containing raw sales data, or None if extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )

            # In production: Read from actual data source
            # sales_df = self.spark.read.jdbc(
            #     url="jdbc:...",
            #     table="zsales_raw",
            #     properties={...}
            # ).filter(
            #     (col("trans_date") >= from_date) &
            #     (col("trans_date") <= to_date) &
            #     (col("status") == "N")
            # )

            # For demonstration: Create sample data
            # Migrates ABAP VALUE #(...) to Python list of tuples
            sample_data = [
                ("T000001", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST001", 
                 "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
                ("T000002", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST002", 
                 "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
                ("T000003", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST003", 
                 "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
                ("T000004", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST001", 
                 "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
                ("T000005", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST004", 
                 "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ]

            # Create DataFrame with schema
            schema = self.get_raw_sales_schema()
            sales_df = self.spark.createDataFrame(sample_data, schema)

            # Get record count
            record_count = sales_df.count()

            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )

            return sales_df

        except Exception as ex:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(ex)}"
            )
            return None