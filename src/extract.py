"""
Data Extractor - extracts raw sales data from source
Migrated from ABAP ZCL_ETL_EXTRACTOR
"""

import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.config import Config
from src.etl_logger import ETLLogger


class Extractor:
    """Extracts raw sales data from source."""
    
    def __init__(self, spark: SparkSession, config: Config, etl_logger: ETLLogger):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            etl_logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.etl_logger = etl_logger
        self.logger = logging.getLogger(__name__)
    
    def get_raw_sales_schema(self) -> StructType:
        """Get schema for raw sales data."""
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
        ])
    
    def extract_data(self, from_date: datetime, to_date: datetime) -> DataFrame:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame with raw sales data
        """
        try:
            self.etl_logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date.date()} to {to_date.date()}'
            )
            
            self.logger.info(f"Extracting data from {from_date} to {to_date}")
            
            # In production, read from actual data source
            # For example: JDBC, Parquet, Delta Lake, etc.
            # df = self.spark.read \
            #     .format(self.config.source_format) \
            #     .option("url", self.config.source_url) \
            #     .option("dbtable", "zsales_raw") \
            #     .load() \
            #     .filter(
            #         (col("trans_date") >= from_date) &
            #         (col("trans_date") <= to_date) &
            #         (col("status") == "N")
            #     )
            
            # For demonstration, create sample data
            sample_data = [
                ("T000001", from_date.date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
                ("T000002", from_date.date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
                ("T000003", from_date.date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
                ("T000004", from_date.date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
                ("T000005", from_date.date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ]
            
            df = self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())
            
            # Cache the DataFrame for better performance
            df.cache()
            
            record_count = df.count()
            
            self.etl_logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.etl_logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise