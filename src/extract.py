"""
ETL Extract Module - Sales Data Extraction from Source
Extracts raw sales data from source database tables
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from datetime import datetime
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.config import Config


class SalesExtractor:
    """Extract raw sales data from source tables"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Config):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration object
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data
        
        Returns:
            StructType schema for raw sales
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
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source (adjust based on actual source)
            if self.config.source_type == "jdbc":
                df = self._extract_from_jdbc(from_date, to_date)
            elif self.config.source_type == "parquet":
                df = self._extract_from_parquet(from_date, to_date)
            elif self.config.source_type == "csv":
                df = self._extract_from_csv(from_date, to_date)
            else:
                # Demo mode - generate sample data
                df = self._generate_sample_data()
            
            # Filter by date range and status
            df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df, True
            
        except Exception as e:
            self.log.error(f"Extraction failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], self.get_raw_sales_schema()), False
    
    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from JDBC source"""
        query = f"""
        (SELECT trans_id, trans_date, customer_id, product_id, quantity, 
                unit_price, currency, sales_rep, region, status
         FROM {self.config.source_table}
         WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
         AND status = 'N') as sales_data
        """
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.config.jdbc_user) \
            .option("password", self.config.jdbc_password) \
            .option("driver", self.config.jdbc_driver) \
            .load()
    
    def _extract_from_parquet(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from Parquet files"""
        return self.spark.read \
            .schema(self.get_raw_sales_schema()) \
            .parquet(self.config.source_path)
    
    def _extract_from_csv(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from CSV files"""
        return self.spark.read \
            .schema(self.get_raw_sales_schema()) \
            .option("header", "true") \
            .csv(self.config.source_path)
    
    def _generate_sample_data(self) -> DataFrame:
        """Generate sample data for demo/testing"""
        from decimal import Decimal
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, self.get_raw_sales_schema())