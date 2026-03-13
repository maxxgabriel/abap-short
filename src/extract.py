"""
ETL Extractor Module
Extracts raw sales data from source based on date range.
"""

import logging
from datetime import date, datetime
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

logger = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extracts raw sales data from source table."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
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
            StructField("status", StringType(), False),
        ])
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        run_id: str
    ) -> Optional[DataFrame]:
        """
        Extract sales data for date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            run_id: ETL run identifier
            
        Returns:
            DataFrame with extracted data or None on failure
        """
        try:
            logger.info(f"[{run_id}] Starting extraction from {from_date} to {to_date}")
            
            source_table = self.config.get('source_table', 'zsales_raw')
            
            # Read from source table
            df = self.spark.read \
                .format(self.config.get('source_format', 'jdbc')) \
                .option("url", self.config['jdbc_url']) \
                .option("dbtable", source_table) \
                .option("user", self.config.get('db_user')) \
                .option("password", self.config.get('db_password')) \
                .option("driver", self.config.get('jdbc_driver', 'com.sap.db.jdbc.Driver')) \
                .load()
            
            # Filter by date range and status
            filtered_df = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            record_count = filtered_df.count()
            logger.info(f"[{run_id}] Extracted {record_count} records successfully")
            
            return filtered_df
            
        except Exception as e:
            logger.error(f"[{run_id}] Extraction failed: {str(e)}", exc_info=True)
            return None
    
    def create_sample_data(self, run_id: str) -> DataFrame:
        """
        Create sample data for testing.
        
        Args:
            run_id: ETL run identifier
            
        Returns:
            DataFrame with sample data
        """
        logger.info(f"[{run_id}] Creating sample data for testing")
        
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.schema)
        logger.info(f"[{run_id}] Created {df.count()} sample records")
        
        return df