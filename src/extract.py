"""
Data extraction module for Sales ETL system.
Extracts raw sales data from source systems.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Tuple
import logging

from src.logger import ETLLogger


class DataExtractor:
    """Extracts raw sales data from source tables."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self._raw_sales_schema = self._get_raw_sales_schema()
    
    def _get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data."""
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_table: str = "zsales_raw"
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for the given date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_table: Source table name
            
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source table
            # In production, this would read from actual database
            df = self.spark.read \
                .format("delta") \
                .table(source_table) \
                .filter(f"trans_date >= '{from_date}' AND trans_date <= '{to_date}'") \
                .filter("status = 'N'")
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df, True
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], self._raw_sales_schema), False
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample raw sales data for testing.
        
        Returns:
            DataFrame with sample data
        """
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, 
             "USD", "John Doe", "NORTH", "N", datetime.now(), "SYSTEM"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99,
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99,
             "USD", "John Doe", "EAST", "N", datetime.now(), "SYSTEM"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99,
             "USD", "Bob Wilson", "WEST", "N", datetime.now(), "SYSTEM"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99,
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM")
        ]
        
        return self.spark.createDataFrame(sample_data, self._raw_sales_schema)