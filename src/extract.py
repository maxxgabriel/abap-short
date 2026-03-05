"""
ETL Extractor Module - Extracts raw sales data
Converts ABAP ZCL_ETL_EXTRACTOR to PySpark operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, lit
from typing import Tuple, Optional
from datetime import date

from src.logger import ETLLogger


class ETLExtractor:
    """
    Extracts raw sales data from source (ABAP ZCL_ETL_EXTRACTOR equivalent).
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor.

        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger

    @staticmethod
    def get_raw_sales_schema() -> StructType:
        """
        Define schema for raw sales data (ZSALES_RAW table mapping).

        Returns:
            StructType schema for raw sales
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
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])

    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: Optional[str] = None
    ) -> Tuple[bool, Optional[DataFrame]]:
        """
        Extract raw sales data from source.

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Optional path to source data (Delta/Parquet/CSV)

        Returns:
            Tuple of (success_flag, extracted_dataframe)
        """
        try:
            self.logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )

            # Read from source (Delta Lake, Parquet, etc.)
            if source_path:
                df = self._read_from_source(source_path, from_date, to_date)
            else:
                # Generate sample data for demonstration
                df = self._generate_sample_data()

            # Filter by date range and status
            df = df.filter(
                (df.trans_date >= lit(from_date)) &
                (df.trans_date <= lit(to_date)) &
                (df.status == lit(ETLLogger.STATUS_NEW))
            )

            record_count = df.count()

            self.logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )

            return True, df

        except Exception as e:
            self.logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return False, None

    def _read_from_source(
        self,
        source_path: str,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Read data from actual source (Delta, Parquet, etc.).

        Args:
            source_path: Path to source data
            from_date: Start date filter
            to_date: End date filter

        Returns:
            DataFrame with extracted data
        """
        # Detect format from path
        if source_path.endswith('.delta') or '/delta/' in source_path:
            df = self.spark.read.format("delta").load(source_path)
        elif source_path.endswith('.parquet'):
            df = self.spark.read.parquet(source_path)
        elif source_path.endswith('.csv'):
            df = self.spark.read.csv(source_path, header=True, schema=self.get_raw_sales_schema())
        else:
            # Default to Delta
            df = self.spark.read.format("delta").load(source_path)

        return df

    def _generate_sample_data(self) -> DataFrame:
        """
        Generate sample data for demonstration (matching ABAP sample data).

        Returns:
            DataFrame with sample sales data
        """
        from datetime import datetime
        from decimal import Decimal

        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, Decimal("99.99"),
             "USD", "John Doe", "NORTH", "N", datetime.now(), "system"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, Decimal("149.99"),
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "system"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, Decimal("99.99"),
             "USD", "John Doe", "EAST", "N", datetime.now(), "system"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, Decimal("299.99"),
             "USD", "Bob Wilson", "WEST", "N", datetime.now(), "system"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, Decimal("149.99"),
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "system"),
        ]

        return self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())