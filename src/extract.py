"""
Extract module for Sales ETL Pipeline
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extracts raw sales data from source table"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize extractor
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.schema = self._get_raw_sales_schema()
    
    @staticmethod
    def _get_raw_sales_schema() -> StructType:
        """Define schema for raw sales data"""
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
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        etl_run_id: str
    ) -> DataFrame:
        """
        Extract raw sales data for date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            etl_run_id: Unique ETL run identifier
            
        Returns:
            DataFrame with raw sales data
        """
        try:
            logger.info(f"Starting extraction from {from_date} to {to_date}")
            logger.info(f"ETL Run ID: {etl_run_id}")
            
            source_path = self.config['source']['path']
            source_format = self.config['source']['format']
            
            # Read from source
            df = self._read_source_data(source_path, source_format)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')  # Only new records
            )
            
            # Add extraction metadata
            df_with_metadata = df_filtered.withColumn(
                "extraction_timestamp",
                df_filtered.created_at.cast("timestamp")
            )
            
            record_count = df_with_metadata.count()
            logger.info(f"Extracted {record_count} records")
            
            return df_with_metadata
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _read_source_data(self, path: str, format_type: str) -> DataFrame:
        """Read data from source based on format"""
        if format_type.lower() == 'parquet':
            return self.spark.read.parquet(path)
        elif format_type.lower() == 'csv':
            return self.spark.read.csv(
                path, 
                header=True, 
                schema=self.schema
            )
        elif format_type.lower() == 'jdbc':
            return self._read_from_jdbc(path)
        else:
            raise ValueError(f"Unsupported source format: {format_type}")
    
    def _read_from_jdbc(self, table_name: str) -> DataFrame:
        """Read from JDBC source (SAP HANA, etc.)"""
        jdbc_config = self.config['source']['jdbc']
        
        return self.spark.read.format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", table_name) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
    
    def create_sample_data(self) -> DataFrame:
        """Create sample data for testing (mimics ABAP sample data)"""
        from pyspark.sql.functions import current_date
        
        sample_data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, 
             "USD", "John Doe", "NORTH", "N", datetime.now(), "SYSTEM"),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, 
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, 
             "USD", "John Doe", "EAST", "N", datetime.now(), "SYSTEM"),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, 
             "USD", "Bob Wilson", "WEST", "N", datetime.now(), "SYSTEM"),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, 
             "USD", "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM")
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.schema)
        return df.withColumn("trans_date", current_date())