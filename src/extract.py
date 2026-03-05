"""
Extract Module - Sales ETL System
Extracts raw sales data from source tables
"""
from typing import List, Tuple
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
import logging

from src.logger import ETLLogger
from src.schemas import RawSalesSchema


class ETLExtractor:
    """Extracts raw sales data from source"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
        """
        self.spark = spark
        self.logger = logger
        self.log = logging.getLogger(__name__)
    
    def extract_data(
        self, 
        from_date: date, 
        to_date: date,
        source_table: str = "zsales_raw"
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data within date range
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name
            
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # In production, read from actual table
            # df = self.spark.read.table(source_table)
            # df = df.filter(
            #     (df.trans_date >= from_date) & 
            #     (df.trans_date <= to_date) &
            #     (df.status == 'N')
            # )
            
            # For demonstration, create sample data
            df = self._create_sample_data()
            
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
            self.log.error(f"Extraction failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], RawSalesSchema.get_schema()), False
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for demonstration"""
        from datetime import datetime
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        df = self.spark.createDataFrame(sample_data, RawSalesSchema.get_schema())
        return df