"""
Data Extraction Module
Extracts raw sales data from source systems.
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import ExtractError


class Extractor:
    """
    Handles extraction of raw sales data from source systems.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger instance
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract sales data for the given date range.
        
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
            
            # Define schema for raw sales data
            schema = self._get_raw_sales_schema()
            
            # In production, this would query from a database or file system
            # For demonstration, create sample data
            sample_data = self._create_sample_data()
            
            df = self.spark.createDataFrame(sample_data, schema=schema)
            
            # Filter by date range
            df = df.filter(
                (df.trans_date >= from_date) & (df.trans_date <= to_date)
            )
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(
                error_text=f"Failed to extract data: {str(e)}",
                error_step="EXTRACT"
            ) from e
    
    def _get_raw_sales_schema(self) -> StructType:
        """
        Get the schema for raw sales data.
        
        Returns:
            StructType schema definition
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
    
    def _create_sample_data(self) -> list:
        """
        Create sample raw sales data for demonstration.
        
        Returns:
            List of tuples with sample data
        """
        today = datetime.now().date()
        
        return [
            ("T000001", today, "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", today, "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", today, "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", today, "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", today, "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000006", today, "CUST005", "PROD001", 8, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000007", today, "CUST002", "PROD003", 12, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000008", today, "CUST006", "PROD002", 25, 149.99, "USD", "Jane Smith", "EAST", "N"),
        ]