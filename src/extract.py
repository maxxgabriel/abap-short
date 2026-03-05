"""
Sales ETL - Data Extraction Module
Extracts raw sales data from source systems
"""
from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
import logging
from src.logger import ETLLogger
from src.config import ETLConfig


class SalesExtractor:
    """Handles extraction of raw sales data"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
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
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: str = None
    ) -> DataFrame:
        """
        Extract raw sales data for date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional path override for source data
            
        Returns:
            DataFrame containing extracted sales records
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Use config path if not overridden
            if source_path is None:
                source_path = self.config.get("source.path")
            
            # Read source data
            df = self._read_source_data(source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date.between(from_date, to_date)) &
                (df.status == "N")
            )
            
            # Cache for performance
            df_filtered.cache()
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise
    
    def _read_source_data(self, source_path: str) -> DataFrame:
        """Read data from source system"""
        source_format = self.config.get("source.format", "parquet")
        
        if source_format == "parquet":
            return self.spark.read.schema(self.schema).parquet(source_path)
        elif source_format == "csv":
            return self.spark.read.schema(self.schema).option("header", "true").csv(source_path)
        elif source_format == "delta":
            return self.spark.read.format("delta").load(source_path)
        elif source_format == "jdbc":
            return self._read_from_database()
        else:
            raise ValueError(f"Unsupported source format: {source_format}")
    
    def _read_from_database(self) -> DataFrame:
        """Read data from database using JDBC"""
        jdbc_config = self.config.get("source.jdbc")
        
        return self.spark.read.format("jdbc").options(
            url=jdbc_config["url"],
            dbtable=jdbc_config["table"],
            user=jdbc_config["user"],
            password=jdbc_config["password"],
            driver=jdbc_config["driver"]
        ).load()
    
    def create_sample_data(self) -> DataFrame:
        """Create sample data for testing"""
        sample_data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N", 
             datetime.now(), "SYSTEM"),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N",
             datetime.now(), "SYSTEM"),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N",
             datetime.now(), "SYSTEM"),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N",
             datetime.now(), "SYSTEM"),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N",
             datetime.now(), "SYSTEM")
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.schema)