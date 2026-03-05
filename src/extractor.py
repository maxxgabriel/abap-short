"""
ETL Extractor Module
Extracts raw sales data from source systems.
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.exceptions import ExtractError


class ETLExtractor:
    """
    Extracts raw sales data from source tables or files.
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        config: Dict[str, Any]
    ):
        """
        Initialize the extractor.

        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Extractor configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.statistics = {
            'records_extracted': 0,
            'records_filtered': 0
        }

    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data for the specified date range.

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
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )

            # Get source configuration
            source_type = self.config.get('source_type', 'table')
            source_path = self.config.get('source_path')

            if source_type == 'table':
                raw_data = self._extract_from_table(from_date, to_date)
            elif source_type == 'file':
                raw_data = self._extract_from_file(source_path, from_date, to_date)
            else:
                raw_data = self._generate_sample_data()

            # Cache for performance
            raw_data.cache()

            count = raw_data.count()
            self.statistics['records_extracted'] = count

            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=count,
                records_success=count,
                message=f'Extracted {count} records successfully'
            )

            return raw_data

        except Exception as e:
            raise ExtractError(
                error_text=f"Extraction failed: {str(e)}",
                error_step='EXTRACT'
            )

    def _extract_from_table(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from database table.

        Args:
            from_date: Start date
            to_date: End date

        Returns:
            DataFrame with extracted data
        """
        table_name = self.config.get('table_name', 'sales_raw')
        status_filter = self.config.get('status_filter', 'N')

        # Read from table (example - adjust based on your database)
        df = self.spark.read.table(table_name)

        # Apply filters
        df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date) &
            (df.status == status_filter)
        )

        return df

    def _extract_from_file(
        self,
        source_path: str,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """
        Extract data from file source.

        Args:
            source_path: Path to source file(s)
            from_date: Start date
            to_date: End date

        Returns:
            DataFrame with extracted data
        """
        file_format = self.config.get('file_format', 'parquet')

        df = self.spark.read.format(file_format).load(source_path)

        # Apply date filters
        df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date)
        )

        return df

    def _generate_sample_data(self) -> DataFrame:
        """
        Generate sample data for testing.

        Returns:
            DataFrame with sample sales data
        """
        schema = StructType([
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

        from datetime import date

        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]

        return self.spark.createDataFrame(sample_data, schema)

    def get_statistics(self) -> Dict[str, int]:
        """
        Get extraction statistics.

        Returns:
            Dictionary with statistics
        """
        return self.statistics.copy()