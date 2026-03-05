"""
Data extraction module for Sales ETL pipeline.
Reads raw sales data from source tables.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class SalesExtractor:
    """Extracts raw sales data from source system."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize extractor with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema matching ZSALES_RAW table structure
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
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_table: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            source_table: Optional source table name override
            
        Returns:
            DataFrame containing raw sales records
        """
        table_name = source_table or self.config['source']['table_name']
        
        self.logger.info(
            f"Starting extraction from {from_date} to {to_date} from table {table_name}"
        )
        
        try:
            # Read from source table with filter
            df = self.spark.read.table(table_name)
            
            # Apply date range and status filters
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
    
    def extract_sample_data(self) -> DataFrame:
        """
        Create sample data for testing/demonstration.
        
        Returns:
            DataFrame with sample sales records
        """
        schema = self.get_raw_sales_schema()
        
        sample_data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema)
        self.logger.info(f"Created sample data with {df.count()} records")
        
        return df