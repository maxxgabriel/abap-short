"""
Data Extraction Module
Extracts raw sales data from source systems.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql import functions as F
import yaml
import logging


class DataExtractor:
    """Extracts raw sales data from various sources."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize extractor with configuration."""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.spark = self._create_spark_session()
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session."""
        return (SparkSession.builder
                .appName("SalesDataExtractor")
                .getOrCreate())
    
    def get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data."""
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False)
        ])
    
    def extract_sales_data(self, 
                          from_date: str, 
                          to_date: str,
                          status: str = 'N') -> DataFrame:
        """
        Extract sales data from source.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            status: Status filter ('N' = New)
            
        Returns:
            DataFrame with raw sales data
        """
        self.logger.info(f"Extracting sales data from {from_date} to {to_date}")
        
        source_config = self.config['source']
        
        # Read from configured source
        df = (self.spark.read
              .format(source_config['format'])
              .schema(self.get_raw_sales_schema())
              .load(source_config['path']))
        
        # Apply filters
        df = df.filter(
            (F.col('trans_date').between(from_date, to_date)) &
            (F.col('status') == status)
        )
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def close(self):
        """Close Spark session."""
        if self.spark:
            self.spark.stop()