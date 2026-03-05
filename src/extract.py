"""
Data extraction component for Sales ETL system.
Converted from ABAP ZCL_ETL_EXTRACTOR.
"""

from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.constants import ETLConstants
from src.logger import ETLLogger
from src.schemas import ETLSchemas, ProcessSteps, StatusCodes


class ETLExtractor:
    """Extracts raw sales data from source."""

    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger

    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data for date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Optional path to source data
            
        Returns:
            DataFrame with raw sales data, or None on failure
        """
        try:
            self.logger.log_message(
                step=ProcessSteps.EXTRACT,
                status=StatusCodes.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}",
            )

            # Read from source (table, file, etc.)
            if source_path:
                df = self._read_from_file(source_path)
            else:
                df = self._read_from_table()

            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date)
                & (df.trans_date <= to_date)
                & (df.status == StatusCodes.NEW)
            )

            record_count = df_filtered.count()

            self.logger.log_message(
                step=ProcessSteps.EXTRACT,
                status=StatusCodes.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully",
            )

            return df_filtered

        except Exception as e:
            self.logger.log_message(
                step=ProcessSteps.EXTRACT,
                status=StatusCodes.ERROR,
                message=f"Extraction failed: {str(e)}",
            )
            return None

    def _read_from_file(self, source_path: str) -> DataFrame:
        """
        Read raw sales data from file.
        
        Args:
            source_path: Path to source file
            
        Returns:
            DataFrame with raw sales data
        """
        schema = ETLSchemas.raw_sales_schema()
        
        # Read based on file format
        if source_path.endswith(".parquet"):
            return self.spark.read.schema(schema).parquet(source_path)
        elif source_path.endswith(".csv"):
            return self.spark.read.schema(schema).csv(source_path, header=True)
        elif source_path.endswith(".json"):
            return self.spark.read.schema(schema).json(source_path)
        else:
            # Default to parquet
            return self.spark.read.schema(schema).parquet(source_path)

    def _read_from_table(self) -> DataFrame:
        """
        Read raw sales data from database table.
        
        Returns:
            DataFrame with raw sales data
        """
        # In production, this would read from actual database
        # Example: return self.spark.read.jdbc(url, "zsales_raw", properties)
        
        # For now, create sample data matching ABAP implementation
        schema = ETLSchemas.raw_sales_schema()
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", 
             "John Doe", "NORTH", "N", None, None),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", None, None),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD",
             "John Doe", "EAST", "N", None, None),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD",
             "Bob Wilson", "WEST", "N", None, None),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", None, None),
        ]
        
        return self.spark.createDataFrame(sample_data, schema)