"""
ETL Extractor Module
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Tuple
import logging


class ETLExtractor:
    """Extracts raw sales data from source table"""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger):
        """
        Initialize the extractor
        
        Args:
            spark: SparkSession instance
            logger: Logger instance for logging
        """
        self.spark = spark
        self.logger = logger
        self._schema = self._get_raw_sales_schema()
    
    def _get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data"""
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
    
    def extract_data(self, from_date: str, to_date: str) -> Tuple[bool, DataFrame]:
        """
        Extract sales data for the given date range
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            Tuple of (success_flag, dataframe)
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # Read from source table (replace with actual source)
            # In production, this would read from database/data lake
            df = self.spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/sales_db") \
                .option("dbtable", "zsales_raw") \
                .option("user", "etl_user") \
                .option("password", "etl_password") \
                .schema(self._schema) \
                .load()
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) & 
                (df.status == 'N')
            )
            
            record_count = df_filtered.count()
            
            self.logger.info(
                f"Extracted {record_count} records successfully",
                extra={
                    'step': 'EXTRACT',
                    'status': 'S',
                    'records_processed': record_count,
                    'records_success': record_count
                }
            )
            
            return True, df_filtered
            
        except Exception as e:
            self.logger.error(
                f"Extraction failed: {str(e)}",
                extra={'step': 'EXTRACT', 'status': 'E'},
                exc_info=True
            )
            return False, None
    
    def extract_sample_data(self) -> Tuple[bool, DataFrame]:
        """
        Extract sample data for testing
        
        Returns:
            Tuple of (success_flag, dataframe)
        """
        try:
            self.logger.info("Creating sample data for testing")
            
            # Create sample data
            sample_data = [
                ('T000001', datetime.now().date(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
                ('T000002', datetime.now().date(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
                ('T000003', datetime.now().date(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
                ('T000004', datetime.now().date(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
                ('T000005', datetime.now().date(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N')
            ]
            
            df = self.spark.createDataFrame(sample_data, schema=self._schema)
            
            self.logger.info(
                f"Created {df.count()} sample records",
                extra={
                    'step': 'EXTRACT',
                    'status': 'S',
                    'records_processed': df.count()
                }
            )
            
            return True, df
            
        except Exception as e:
            self.logger.error(
                f"Sample data creation failed: {str(e)}",
                extra={'step': 'EXTRACT', 'status': 'E'},
                exc_info=True
            )
            return False, None