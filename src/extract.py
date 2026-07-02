"""
PySpark Data Extraction Module
Extracts raw sales data from source systems.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, Optional
import logging
from datetime import datetime


class DataExtractor:
    """Handles extraction of raw sales data from source."""
    
    def __init__(self, spark: SparkSession, config: Dict, logger: logging.Logger):
        """
        Initialize DataExtractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.extract_config = config.get('extract', {})
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema for raw sales table
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_data(self, from_date: str, to_date: str) -> Dict:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary containing DataFrame and extraction statistics
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # Get source configuration
            source_type = self.extract_config.get('source_type', 'parquet')
            source_path = self.extract_config.get('source_path', 'input/raw_sales')
            
            # Read data based on source type
            if source_type == 'parquet':
                df = self._extract_from_parquet(source_path, from_date, to_date)
            elif source_type == 'csv':
                df = self._extract_from_csv(source_path, from_date, to_date)
            elif source_type == 'jdbc':
                df = self._extract_from_database(from_date, to_date)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Filter by date range and status
            df_filtered = df.filter(
                (col("trans_date") >= lit(from_date)) &
                (col("trans_date") <= lit(to_date)) &
                (col("status") == lit("N"))
            )
            
            record_count = df_filtered.count()
            
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return {
                'success': True,
                'dataframe': df_filtered,
                'record_count': record_count,
                'message': f'Extracted {record_count} records from {from_date} to {to_date}'
            }
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'dataframe': None,
                'record_count': 0,
                'message': f'Extraction failed: {str(e)}'
            }
    
    def _extract_from_parquet(self, source_path: str, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from Parquet files.
        
        Args:
            source_path: Path to Parquet files
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data
        """
        self.logger.info(f"Reading from Parquet: {source_path}")
        
        df = self.spark.read.schema(self.get_raw_sales_schema()).parquet(source_path)
        
        return df
    
    def _extract_from_csv(self, source_path: str, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from CSV files.
        
        Args:
            source_path: Path to CSV files
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data
        """
        self.logger.info(f"Reading from CSV: {source_path}")
        
        df = self.spark.read \
            .schema(self.get_raw_sales_schema()) \
            .option("header", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(source_path)
        
        return df
    
    def _extract_from_database(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from database using JDBC.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data
        """
        db_config = self.extract_config.get('database', {})
        jdbc_url = db_config.get('jdbc_url')
        table_name = db_config.get('table_name', 'raw_sales')
        
        if not jdbc_url:
            raise ValueError("JDBC URL not configured")
        
        self.logger.info(f"Reading from database table: {table_name}")
        
        # Build query with date filter
        query = f"""
            (SELECT * FROM {table_name}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') AS sales_data
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", db_config.get('user')) \
            .option("password", db_config.get('password')) \
            .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing.
        
        Returns:
            DataFrame with sample sales data
        """
        self.logger.info("Creating sample data")
        
        sample_data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N", None, None),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N", None, None),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N", None, None),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N", None, None),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N", None, None),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())
        
        return df


def create_extractor(spark: SparkSession, config: Dict, logger: logging.Logger) -> DataExtractor:
    """
    Factory function to create DataExtractor instance.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        logger: Logger instance
        
    Returns:
        DataExtractor instance
    """
    return DataExtractor(spark, config, logger)