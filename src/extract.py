"""
Data extraction module for Sales ETL pipeline.
Extracts raw sales data from source systems.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Tuple
import logging


class SalesDataExtractor:
    """Extracts raw sales data from source tables."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_raw_sales_schema(self) -> StructType:
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
    
    def extract_data(self, from_date: str, to_date: str) -> Tuple[DataFrame, dict]:
        """
        Extract raw sales data for the given date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            
        Returns:
            Tuple of (DataFrame with extracted data, metrics dictionary)
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            source_path = self.config['source']['raw_sales_path']
            source_format = self.config['source'].get('format', 'parquet')
            
            # Read source data
            df = self.spark.read \
                .format(source_format) \
                .schema(self.get_raw_sales_schema()) \
                .load(source_path)
            
            # Filter by date range and status
            filtered_df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            # Cache for performance
            filtered_df.cache()
            
            # Calculate metrics
            record_count = filtered_df.count()
            
            metrics = {
                'records_extracted': record_count,
                'from_date': from_date,
                'to_date': to_date,
                'extraction_time': datetime.now().isoformat()
            }
            
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return filtered_df, metrics
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def validate_extracted_data(self, df: DataFrame) -> Tuple[bool, list]:
        """
        Validate extracted data quality.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Tuple of (validation success boolean, list of validation messages)
        """
        validation_messages = []
        
        # Check for null values in required fields
        null_checks = ['trans_id', 'trans_date', 'customer_id', 'product_id', 'quantity', 'unit_price']
        for col in null_checks:
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                validation_messages.append(f"Found {null_count} null values in {col}")
        
        # Check for negative quantities
        negative_qty = df.filter(df.quantity < 0).count()
        if negative_qty > 0:
            validation_messages.append(f"Found {negative_qty} records with negative quantities")
        
        # Check for zero/negative prices
        invalid_price = df.filter(df.unit_price <= 0).count()
        if invalid_price > 0:
            validation_messages.append(f"Found {invalid_price} records with invalid prices")
        
        is_valid = len(validation_messages) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation issues: {validation_messages}")
        
        return is_valid, validation_messages