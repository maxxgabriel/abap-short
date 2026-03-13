"""
Extract module for Sales ETL process.
Extracts raw sales data from source system.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from datetime import datetime
import logging


class SalesExtractor:
    """Handles extraction of raw sales data."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_schema(self) -> StructType:
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
    
    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract sales data from source.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame containing raw sales data
        """
        self.logger.info(f"Starting extraction from {from_date} to {to_date}")
        
        try:
            source_path = self.config['extract']['source_path']
            source_format = self.config['extract']['source_format']
            
            # Read data with schema
            df = self.spark.read \
                .format(source_format) \
                .schema(self.get_schema()) \
                .option("header", "true") \
                .load(source_path)
            
            # Filter by date range and status
            filtered_df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            record_count = filtered_df.count()
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise