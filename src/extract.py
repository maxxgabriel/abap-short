"""
Data Extractor - Extract raw sales data
Migrated from ABAP ZCL_ETL_EXTRACTOR
"""
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
import logging

from src.config import ETLConfig


class Extractor:
    """
    Extracts raw sales data from source.
    Replaces ZCL_ETL_EXTRACTOR.
    """
    
    def __init__(self, spark: SparkSession, config: ETLConfig, logger: logging.Logger, etl_run_id: str):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            logger: Logger instance
            etl_run_id: ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.etl_run_id = etl_run_id
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        Replaces ty_raw_sales type definition.
        
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
        ])
    
    def extract_data(self, from_date: datetime, to_date: datetime) -> Optional[DataFrame]:
        """
        Extract raw sales data for the given date range.
        Replaces extract_data method from ABAP.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data, or None if failed
        """
        try:
            self.logger.info(
                f"Starting extraction from {from_date.date()} to {to_date.date()}",
                extra={'step': self.config.steps.EXTRACT, 'status': self.config.status.INFO}
            )
            
            # Read from source (in production, this would be actual database or file)
            # For now, we create sample data similar to ABAP version
            schema = self.get_raw_sales_schema()
            
            # Sample data creation (replace with actual data source)
            sample_data = [
                ('T000001', from_date.date(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
                ('T000002', from_date.date(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
                ('T000003', from_date.date(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
                ('T000004', from_date.date(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
                ('T000005', from_date.date(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ]
            
            df = self.spark.createDataFrame(sample_data, schema)
            
            # In production, read from actual source:
            # df = self.spark.read \
            #     .format("jdbc") \
            #     .option("url", "jdbc:...") \
            #     .option("dbtable", "zsales_raw") \
            #     .option("user", "...") \
            #     .option("password", "...") \
            #     .load() \
            #     .filter(
            #         (col("trans_date") >= from_date.date()) &
            #         (col("trans_date") <= to_date.date()) &
            #         (col("status") == self.config.status.NEW)
            #     )
            
            count = df.count()
            
            self.logger.info(
                f"Extracted {count} records successfully",
                extra={
                    'step': self.config.steps.EXTRACT,
                    'status': self.config.status.SUCCESS,
                    'records_processed': count,
                    'records_success': count
                }
            )
            
            return df
            
        except Exception as ex:
            self.logger.error(
                f"Extraction failed: {str(ex)}",
                extra={'step': self.config.steps.EXTRACT, 'status': self.config.status.ERROR},
                exc_info=True
            )
            return None