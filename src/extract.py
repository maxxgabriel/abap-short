"""
Data extraction module for Sales ETL Pipeline.
Extracts raw sales data from source systems.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from datetime import datetime
from typing import Optional
import logging

from src.logger import ETLLogger


class SalesExtractor:
    """Extracts raw sales data from source tables."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
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
        source_path: str,
        from_date: str,
        to_date: str,
        file_format: str = "parquet"
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data from source.
        
        Args:
            source_path: Path to source data
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            file_format: Source file format (parquet, csv, delta)
        
        Returns:
            DataFrame containing raw sales data or None on failure
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read data from source
            if file_format == "parquet":
                df = self.spark.read.schema(self.schema).parquet(source_path)
            elif file_format == "csv":
                df = self.spark.read.schema(self.schema).option("header", "true").csv(source_path)
            elif file_format == "delta":
                df = self.spark.read.format("delta").schema(self.schema).load(source_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == "N")
            )
            
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            logging.error(f"Extraction error: {str(e)}", exc_info=True)
            return None
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing.
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N", datetime.now(), "SYSTEM"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N", datetime.now(), "SYSTEM"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N", datetime.now(), "SYSTEM"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.schema)
        
        self.logger.log_message(
            step="EXTRACT",
            status="I",
            message="Created sample data for testing"
        )
        
        return df