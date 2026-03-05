"""
Data extraction module for Sales ETL System.
Extracts raw sales data from source tables.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from typing import Tuple, Optional
from datetime import date
import logging

from src.logger import ETLLogger
from src.constants import ETLConstants


class DataExtractor:
    """Extracts raw sales data from source systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the data extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.constants = ETLConstants()
    
    @staticmethod
    def get_raw_sales_schema() -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema definition
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
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: Optional[str] = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Optional path to source data (for file-based sources)
        
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.log_message(
                step=self.constants.STEP_EXTRACT,
                status=self.constants.STATUS_INFO,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source (file or database)
            if source_path:
                df = self._extract_from_file(source_path)
            else:
                df = self._extract_from_database()
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == self.constants.STATUS_NEW)
            )
            
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step=self.constants.STEP_EXTRACT,
                status=self.constants.STATUS_SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered, True
            
        except Exception as e:
            self.logger.log_message(
                step=self.constants.STEP_EXTRACT,
                status=self.constants.STATUS_ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            logging.error(f"Extraction error: {str(e)}", exc_info=True)
            return self.spark.createDataFrame([], self.get_raw_sales_schema()), False
    
    def _extract_from_file(self, source_path: str) -> DataFrame:
        """
        Extract data from file source.
        
        Args:
            source_path: Path to source file
        
        Returns:
            DataFrame with raw data
        """
        schema = self.get_raw_sales_schema()
        
        if source_path.endswith('.parquet'):
            return self.spark.read.schema(schema).parquet(source_path)
        elif source_path.endswith('.json'):
            return self.spark.read.schema(schema).json(source_path)
        elif source_path.endswith('.csv'):
            return self.spark.read.schema(schema).option("header", "true").csv(source_path)
        else:
            raise ValueError(f"Unsupported file format: {source_path}")
    
    def _extract_from_database(self) -> DataFrame:
        """
        Extract data from database source.
        
        Returns:
            DataFrame with raw data
        """
        # In production, this would connect to actual database
        # For now, return empty DataFrame with schema
        schema = self.get_raw_sales_schema()
        return self.spark.createDataFrame([], schema)
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing/demonstration.
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import datetime
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        schema = self.get_raw_sales_schema()
        return self.spark.createDataFrame(sample_data, schema)