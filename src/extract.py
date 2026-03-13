"""
PySpark Extractor Module
Migrated from ZCL_ETL_EXTRACTOR ABAP class
Extracts raw sales data from source
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
from typing import Tuple
from datetime import datetime

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesExtractor:
    """
    Extracts raw sales data from source.
    Equivalent to ZCL_ETL_EXTRACTOR ABAP class.
    """

    def __init__(self, logger: ETLLogger, config: ETLConfig):
        """
        Initialize extractor with logger and configuration.
        
        Args:
            logger: ETL logger instance
            config: ETL configuration instance
        """
        self.logger = logger
        self.config = config
        self.spark = SparkSession.getActiveSession()
        if not self.spark:
            raise RuntimeError("No active Spark session found")

    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        Equivalent to ty_raw_sales structure in ABAP.
        
        Returns:
            StructType schema for raw sales DataFrame
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

    def extract_data(self, from_date: str, to_date: str) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data from source.
        Equivalent to extract_data method in ABAP.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            
        Returns:
            Tuple of (sales DataFrame, success boolean)
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )

            # Read from configured source
            if self.config.source_type == 'parquet':
                raw_df = self._extract_from_parquet(from_date, to_date)
            elif self.config.source_type == 'jdbc':
                raw_df = self._extract_from_jdbc(from_date, to_date)
            elif self.config.source_type == 'csv':
                raw_df = self._extract_from_csv(from_date, to_date)
            else:
                raise ValueError(f"Unsupported source type: {self.config.source_type}")

            # Get count
            record_count = raw_df.count()

            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )

            return raw_df, True

        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            return None, False

    def _extract_from_parquet(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from Parquet files.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            Raw sales DataFrame
        """
        df = self.spark.read.parquet(self.config.source_path)
        
        # Filter by date range and status
        df = df.filter(
            (F.col("trans_date").between(from_date, to_date)) &
            (F.col("status") == "N")
        )
        
        return df

    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from JDBC source (e.g., SAP HANA, Oracle).
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            Raw sales DataFrame
        """
        jdbc_url = self.config.jdbc_url
        connection_properties = {
            "user": self.config.jdbc_user,
            "password": self.config.jdbc_password,
            "driver": self.config.jdbc_driver
        }

        # Build query with date filter
        query = f"""
        (SELECT trans_id, trans_date, customer_id, product_id, quantity,
                unit_price, currency, sales_rep, region, status
         FROM {self.config.source_table}
         WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
         AND status = 'N') AS source_data
        """

        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )

        return df

    def _extract_from_csv(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from CSV files.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            Raw sales DataFrame
        """
        df = self.spark.read.csv(
            self.config.source_path,
            header=True,
            schema=self.get_raw_sales_schema()
        )

        # Filter by date range and status
        df = df.filter(
            (F.col("trans_date").between(from_date, to_date)) &
            (F.col("status") == "N")
        )

        return df

    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing.
        Equivalent to the sample data generation in ABAP extract_data.
        
        Returns:
            Sample sales DataFrame
        """
        sample_data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N")
        ]

        return self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())