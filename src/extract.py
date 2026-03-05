"""
Extract module for Sales ETL pipeline.
Handles data extraction from source systems.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Tuple
import logging

from src.logger import ETLLogger


class SalesExtractor:
    """Extracts raw sales data from source systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        
    def get_raw_sales_schema(self) -> StructType:
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
            StructField("status", StringType(), False)
        ])
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: str
    ) -> Tuple[DataFrame, bool]:
        """
        Extract sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Path to source data
            
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read raw sales data
            schema = self.get_raw_sales_schema()
            df = self.spark.read \
                .schema(schema) \
                .option("header", "true") \
                .csv(source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            # Cache for performance
            df_filtered.cache()
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered, True
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            logging.error(f"Extraction error: {str(e)}", exc_info=True)
            return self.spark.createDataFrame([], self.get_raw_sales_schema()), False