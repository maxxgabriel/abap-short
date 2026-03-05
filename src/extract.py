"""
ETL Extractor Module - PySpark DataFrame Implementation
Extracts raw sales data from source systems
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ETLExtractError


class ETLExtractor:
    """
    PySpark implementation of ETL Extractor component
    Reads raw sales data from source tables/files
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize ETL Extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data
        
        Returns:
            StructType schema definition
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
    
    def extract_data(self, from_date: str, to_date: str) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data from source
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source
            source_df = self._read_from_source()
            
            # Filter by date range and status
            filtered_df = source_df.filter(
                (source_df.trans_date >= from_date) &
                (source_df.trans_date <= to_date) &
                (source_df.status == 'N')
            )
            
            record_count = filtered_df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return filtered_df, True
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f"Extraction failed: {str(e)}"
            )
            raise ETLExtractError(f"Data extraction failed: {str(e)}") from e
    
    def _read_from_source(self) -> DataFrame:
        """
        Read data from configured source
        
        Returns:
            DataFrame with raw sales data
        """
        source_type = self.config.get('source_type', 'parquet')
        source_path = self.config.get('source_path', 'data/raw_sales')
        
        if source_type == 'parquet':
            return self.spark.read.parquet(source_path)
        elif source_type == 'csv':
            return self.spark.read.csv(
                source_path,
                schema=self.get_raw_sales_schema(),
                header=True
            )
        elif source_type == 'jdbc':
            return self._read_from_jdbc()
        else:
            # Create sample data for demonstration
            return self._create_sample_data()
    
    def _read_from_jdbc(self) -> DataFrame:
        """Read from JDBC source"""
        jdbc_config = self.config.get('jdbc', {})
        return self.spark.read \
            .format('jdbc') \
            .option('url', jdbc_config.get('url')) \
            .option('dbtable', jdbc_config.get('table')) \
            .option('user', jdbc_config.get('user')) \
            .option('password', jdbc_config.get('password')) \
            .option('driver', jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for testing"""
        from datetime import date
        
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N')
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())