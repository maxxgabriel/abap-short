"""
Data Extraction Module for Sales ETL
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql import functions as F
from typing import Optional
import yaml
import logging


class SalesExtractor:
    """Extracts raw sales data from source"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize extractor with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.warning(f"Could not load config: {e}, using defaults")
            return {}
    
    def get_raw_sales_schema(self) -> StructType:
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_data(self, spark: SparkSession, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range
        
        Args:
            spark: SparkSession instance
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with raw sales data or None if error
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # In production, this would read from actual source
            # For now, create sample data
            sample_data = [
                ("T000001", from_date, "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
                ("T000002", from_date, "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
                ("T000003", from_date, "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
                ("T000004", from_date, "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
                ("T000005", from_date, "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ]
            
            df = spark.createDataFrame(
                sample_data,
                ["trans_id", "trans_date", "customer_id", "product_id", "quantity", 
                 "unit_price", "currency", "sales_rep", "region", "status"]
            )
            
            # Convert date string to date type
            df = df.withColumn("trans_date", F.to_date(F.col("trans_date")))
            
            # Add metadata
            df = df.withColumn("created_at", F.current_timestamp()) \
                   .withColumn("created_by", F.lit("ETL_SYSTEM"))
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            return None