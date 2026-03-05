"""
Data Extraction Module
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql import functions as F
from typing import Dict, Optional
import logging
from datetime import datetime


class SalesDataExtractor:
    """
    Extracts raw sales data from source systems.
    Equivalent to ZCL_ETL_EXTRACTOR in ABAP.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for ZSALES_RAW source table.
        
        Returns:
            StructType: Raw sales schema
        """
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])
    
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        status: str = "N"
    ) -> DataFrame:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            status: Status filter (default: 'N' for new)
            
        Returns:
            DataFrame with extracted sales data
        """
        self.logger.info(f"Extracting data from {from_date} to {to_date}")
        
        source_path = self.config.get('source', {}).get('raw_sales_path')
        source_format = self.config.get('source', {}).get('format', 'delta')
        
        try:
            # Read from source
            df = self.spark.read.format(source_format).load(source_path)
            
            # Apply filters
            filtered_df = df.filter(
                (F.col("trans_date").between(from_date, to_date)) &
                (F.col("status") == status)
            )
            
            record_count = filtered_df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise