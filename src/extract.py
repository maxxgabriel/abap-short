"""
Data Extraction Module
Extracts raw sales data from source (ZSALES_RAW equivalent).
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from pyspark.sql.functions import col, lit
from datetime import datetime, timedelta
import logging


class SalesDataExtractor:
    """
    Extracts raw sales data from source system.
    Implements extraction logic from ZCL_ETL_EXTRACTOR.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Optional logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # Extract configuration
        self.source_table = config['extract']['source_table']
        self.source_format = config['extract']['source_format']
        self.source_path = config['extract']['source_path']
        self.date_column = config['extract']['date_column']
        self.status_filter = config['extract']['status_filter']
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for ZSALES_RAW table mapping.
        
        Returns:
            StructType schema
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
    
    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for the given date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with extracted data, or None on failure
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # Read source data
            df = self._read_source_data()
            
            if df is None:
                self.logger.error("Failed to read source data")
                return None
            
            # Apply filters
            filtered_df = self._apply_filters(df, from_date, to_date)
            
            # Log statistics
            count = filtered_df.count()
            self.logger.info(f"Extracted {count} records successfully")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            return None
    
    def _read_source_data(self) -> Optional[DataFrame]:
        """
        Read data from source based on configuration.
        
        Returns:
            DataFrame with source data
        """
        try:
            if self.source_format == "jdbc":
                return self._read_from_jdbc()
            elif self.source_format in ["parquet", "csv", "json"]:
                return self._read_from_file()
            else:
                raise ValueError(f"Unsupported source format: {self.source_format}")
                
        except Exception as e:
            self.logger.error(f"Failed to read source data: {str(e)}", exc_info=True)
            return None
    
    def _read_from_jdbc(self) -> DataFrame:
        """
        Read data from database via JDBC.
        
        Returns:
            DataFrame with database data
        """
        jdbc_config = self.config['extract']['jdbc']
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config['url'])
              .option("dbtable", self.source_table)
              .option("driver", jdbc_config['driver'])
              .option("user", jdbc_config['user'])
              .option("password", jdbc_config['password'])
              .option("fetchsize", jdbc_config['fetch_size'])
              .load())
        
        return df
    
    def _read_from_file(self) -> DataFrame:
        """
        Read data from file system.
        
        Returns:
            DataFrame with file data
        """
        schema = self.get_raw_sales_schema()
        
        df = (self.spark.read
              .format(self.source_format)
              .schema(schema)
              .option("header", "true")
              .load(self.source_path))
        
        return df
    
    def _apply_filters(self, df: DataFrame, from_date: str, to_date: str) -> DataFrame:
        """
        Apply date and status filters to DataFrame.
        
        Args:
            df: Input DataFrame
            from_date: Start date
            to_date: End date
            
        Returns:
            Filtered DataFrame
        """
        # Date range filter
        filtered_df = df.filter(
            (col(self.date_column) >= lit(from_date)) &
            (col(self.date_column) <= lit(to_date))
        )
        
        # Status filter (only new records)
        filtered_df = filtered_df.filter(col("status") == self.status_filter)
        
        return filtered_df


def create_extractor(spark: SparkSession, config: Dict[str, Any], 
                    logger: Optional[logging.Logger] = None) -> SalesDataExtractor:
    """
    Factory function to create SalesDataExtractor instance.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        logger: Optional logger instance
        
    Returns:
        SalesDataExtractor instance
    """
    return SalesDataExtractor(spark, config, logger)