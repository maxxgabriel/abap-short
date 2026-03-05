"""
Data extraction module for ETL pipeline.
Extracts raw sales data from source systems.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Optional
import logging


class DataExtractor:
    """Handles extraction of raw sales data."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.schema = self._get_raw_sales_schema()
    
    def _get_raw_sales_schema(self) -> StructType:
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
    
    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing raw sales data, or None on failure
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # Read from source (could be CSV, Parquet, Database, etc.)
            source_path = self.config.get('source_path', 'data/raw/sales')
            source_format = self.config.get('source_format', 'parquet')
            
            df = self.spark.read \
                .format(source_format) \
                .schema(self.schema) \
                .load(source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')  # Only new records
            )
            
            record_count = df_filtered.count()
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_data_with_retry(
        self, 
        from_date: str, 
        to_date: str,
        max_retries: int = 3
    ) -> Optional[DataFrame]:
        """
        Extract data with retry logic.
        
        Args:
            from_date: Start date
            to_date: End date
            max_retries: Maximum number of retry attempts
            
        Returns:
            DataFrame or None
        """
        for attempt in range(max_retries):
            try:
                return self.extract_data(from_date, to_date)
            except Exception as e:
                self.logger.warning(f"Extraction attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    self.logger.error("All extraction attempts failed")
                    raise
        return None