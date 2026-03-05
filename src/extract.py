"""
Data Extraction Module
Extracts raw sales data from source (maps to ZCL_ETL_EXTRACTOR).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, current_timestamp
from typing import Dict, Optional
import logging
from datetime import date


class SalesDataExtractor:
    """
    Extracts raw sales data from source.
    Maps to ABAP ZCL_ETL_EXTRACTOR class.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize extractor with Spark session and configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Extract configuration
        self.source_path = config['extractor']['source_path']
        self.source_format = config['extractor']['source_format']
        self.status_new = config['extractor']['status_new']
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for ZSALES_RAW table (ty_raw_sales).
        
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
        ])
    
    def extract_data(
        self,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Extract raw sales data for date range.
        Maps to ABAP extract_data method.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame with extracted sales data
        """
        try:
            self.logger.info(
                f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source
            df = self.spark.read \
                .format(self.source_format) \
                .schema(self.get_raw_sales_schema()) \
                .load(self.source_path)
            
            # Filter by date range and status
            filtered_df = df.filter(
                (col("trans_date") >= from_date) &
                (col("trans_date") <= to_date) &
                (col("status") == self.status_new)
            )
            
            record_count = filtered_df.count()
            
            self.logger.info(
                f"Extracted {record_count} records successfully"
            )
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise ExtractionException(f"Failed to extract data: {str(e)}")


class ExtractionException(Exception):
    """Custom exception for extraction errors."""
    pass