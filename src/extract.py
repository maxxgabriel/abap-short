"""
PySpark Data Extraction Module
Extracts raw sales data from source tables.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from typing import Dict, Optional
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import ExtractionError


class DataExtractor:
    """
    Extracts raw sales data from source with filtering and validation.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict):
        """
        Initialize the data extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Load configuration
        self.source_table = config.get('source_table', 'sales_raw')
        self.source_format = config.get('source_format', 'parquet')
        self.source_path = config.get('source_path', '/data/sales_raw')
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define the raw sales data schema.
        
        Returns:
            StructType schema for raw sales data
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
            StructField("region", StringType(), False),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            
        Returns:
            DataFrame containing raw sales data
            
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Read from source
            if self.config.get('use_table', False):
                df = self.spark.table(self.source_table)
            else:
                df = self.spark.read \
                    .format(self.source_format) \
                    .schema(self.get_raw_sales_schema()) \
                    .load(self.source_path)
            
            # Apply filters
            df_filtered = df.filter(
                (col('trans_date').between(from_date, to_date)) &
                (col('status') == 'N')  # Only new records
            )
            
            # Count records
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise ExtractionError(f"Data extraction failed: {str(e)}") from e