"""
Data extraction module for Sales ETL pipeline.
Extracts raw sales data from source systems.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Tuple
import logging
from datetime import datetime


class SalesExtractor:
    """Handles extraction of raw sales data."""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            logger: Logger instance for tracking operations
        """
        self.spark = spark
        self.logger = logger
        self.schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
        """
        Define the schema for raw sales data.
        
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
    
    def extract_data(
        self,
        source_path: str,
        from_date: str,
        to_date: str,
        source_format: str = "parquet"
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data from source.
        
        Args:
            source_path: Path to source data
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            source_format: Format of source data (parquet, csv, delta, etc.)
        
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            self.logger.info(f"Source path: {source_path}")
            
            # Read source data
            df = self.spark.read.format(source_format).schema(self.schema).load(source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == "N")
            )
            
            record_count = df_filtered.count()
            
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df_filtered, True
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            return self.spark.createDataFrame([], self.schema), False
    
    def extract_sample_data(self) -> Tuple[DataFrame, bool]:
        """
        Generate sample data for testing.
        
        Returns:
            Tuple of (DataFrame with sample data, success flag)
        """
        try:
            self.logger.info("Generating sample data for testing")
            
            sample_data = [
                ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
                ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
                ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
                ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
                ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N")
            ]
            
            df = self.spark.createDataFrame(sample_data, self.schema)
            
            record_count = df.count()
            self.logger.info(f"Generated {record_count} sample records")
            
            return df, True
            
        except Exception as e:
            self.logger.error(f"Sample data generation failed: {str(e)}")
            return self.spark.createDataFrame([], self.schema), False