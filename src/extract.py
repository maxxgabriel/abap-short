"""
ETL Extractor Module
Handles extraction of raw sales data from source systems
"""

from datetime import datetime
from typing import Optional
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import ExtractError


class SalesDataExtractor:
    """
    Extracts raw sales data from source systems.
    
    Responsibilities:
    - Define raw sales data schema
    - Extract data from source tables/files
    - Filter by date range
    - Handle extraction errors
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the sales data extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: ETL configuration object
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    @staticmethod
    def get_raw_sales_schema() -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema definition
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
            StructField("status", StringType(), nullable=False)
        ])
    
    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for specified date range.
        
        Args:
            from_date: Start date (format: YYYY-MM-DD)
            to_date: End date (format: YYYY-MM-DD)
            
        Returns:
            DataFrame containing extracted sales data, or None if extraction fails
            
        Raises:
            ExtractError: If extraction encounters critical error
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source (file, database, etc.)
            raw_df = self._read_from_source(from_date, to_date)
            
            # Validate extracted data
            if raw_df is None:
                raise ExtractError("Failed to read data from source")
            
            record_count = raw_df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return raw_df
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(f"Extraction error: {str(e)}")
    
    def _read_from_source(self, from_date: str, to_date: str) -> DataFrame:
        """
        Read raw sales data from configured source.
        
        Args:
            from_date: Start date filter
            to_date: End date filter
            
        Returns:
            DataFrame containing raw sales data
        """
        source_config = self.config.get_source_config()
        source_type = source_config.get("type", "parquet")
        source_path = source_config.get("path")
        
        if source_type == "jdbc":
            # Read from database
            df = self._read_from_jdbc(source_config, from_date, to_date)
        elif source_type == "parquet":
            # Read from parquet files
            df = self._read_from_parquet(source_path, from_date, to_date)
        elif source_type == "csv":
            # Read from CSV files
            df = self._read_from_csv(source_path, from_date, to_date)
        else:
            # Generate sample data for testing
            df = self._generate_sample_data()
        
        return df
    
    def _read_from_jdbc(self, jdbc_config: dict, from_date: str, to_date: str) -> DataFrame:
        """
        Read data from JDBC source.
        
        Args:
            jdbc_config: JDBC connection configuration
            from_date: Start date filter
            to_date: End date filter
            
        Returns:
            DataFrame from database
        """
        query = f"""
            (SELECT * FROM zsales_raw 
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') as sales_data
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "com.sap.db.jdbc.Driver")) \
            .load()
        
        return df
    
    def _read_from_parquet(self, path: str, from_date: str, to_date: str) -> DataFrame:
        """
        Read data from Parquet files.
        
        Args:
            path: Path to parquet files
            from_date: Start date filter
            to_date: End date filter
            
        Returns:
            DataFrame from parquet
        """
        df = self.spark.read \
            .schema(self.get_raw_sales_schema()) \
            .parquet(path)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date) & 
            (df.status == 'N')
        )
        
        return df
    
    def _read_from_csv(self, path: str, from_date: str, to_date: str) -> DataFrame:
        """
        Read data from CSV files.
        
        Args:
            path: Path to CSV files
            from_date: Start date filter
            to_date: End date filter
            
        Returns:
            DataFrame from CSV
        """
        df = self.spark.read \
            .schema(self.get_raw_sales_schema()) \
            .option("header", "true") \
            .csv(path)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date) & 
            (df.status == 'N')
        )
        
        return df
    
    def _generate_sample_data(self) -> DataFrame:
        """
        Generate sample data for testing purposes.
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())
        
        return df