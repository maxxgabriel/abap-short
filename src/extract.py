"""
Data extraction component.
Migrated from ABAP ZCL_ETL_EXTRACTOR class.
"""
from datetime import date
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame

from src.constants import ProcessStep, StatusCode
from src.logger import ETLLogger
from src.schemas import get_raw_sales_schema


class ETLExtractor:
    """
    Extracts raw sales data from source systems.
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
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: str
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Source data path
            
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.log_message(
                step=ProcessStep.EXTRACT.value,
                status=StatusCode.INFO.value,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read source data
            df = self.spark.read.parquet(source_path)
            
            # Filter by date range and status
            filtered_df = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == StatusCode.NEW.value)
            )
            
            record_count = filtered_df.count()
            
            self.logger.log_message(
                step=ProcessStep.EXTRACT.value,
                status=StatusCode.SUCCESS.value,
                message=f"Extracted {record_count} records successfully",
                records_processed=record_count,
                records_success=record_count
            )
            
            return filtered_df, True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.EXTRACT.value,
                status=StatusCode.ERROR.value,
                message=f"Extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], schema=get_raw_sales_schema()), False
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing.
        
        Returns:
            DataFrame with sample sales data
        """
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N")
        ]
        
        return self.spark.createDataFrame(
            sample_data,
            schema=get_raw_sales_schema()
        )