"""
ETL Extractor module.
Converted from ABAP ZCL_ETL_EXTRACTOR class.
"""

from datetime import date
from typing import List, Tuple
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)

from src.config import CONSTANTS, ETLStep, ETLStatus
from src.logger import ETLLogger
from src.types import RawSalesData


class ETLExtractor:
    """
    Data extraction component for ETL process.
    Extracts raw sales data from source system.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.schema = self._get_raw_sales_schema()
    
    def extract_data(
        self, 
        from_date: date, 
        to_date: date
    ) -> Tuple[bool, DataFrame]:
        """
        Extract raw sales data for date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            Tuple of (success flag, DataFrame with raw sales data)
        """
        try:
            self.logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # In production, read from actual database
            # df = self._read_from_database(from_date, to_date)
            
            # For demonstration, create sample data
            df = self._create_sample_data()
            
            # Filter by date range
            df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == CONSTANTS.status.NEW.value)
            )
            
            record_count = df.count()
            
            self.logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return True, df
            
        except Exception as e:
            self.logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return False, self.spark.createDataFrame([], self.schema)
    
    def _get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample data for demonstration.
        
        Returns:
            DataFrame with sample raw sales data
        """
        from datetime import datetime
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 
             Decimal("99.99"), "USD", "John Doe", "NORTH", "N", 
             datetime.now(), "SYSTEM"),
            ("T000002", date.today(), "CUST002", "PROD002", 5,
             Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N",
             datetime.now(), "SYSTEM"),
            ("T000003", date.today(), "CUST003", "PROD001", 20,
             Decimal("99.99"), "USD", "John Doe", "EAST", "N",
             datetime.now(), "SYSTEM"),
            ("T000004", date.today(), "CUST001", "PROD003", 3,
             Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N",
             datetime.now(), "SYSTEM"),
            ("T000005", date.today(), "CUST004", "PROD002", 15,
             Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N",
             datetime.now(), "SYSTEM"),
        ]
        
        return self.spark.createDataFrame(sample_data, self.schema)
    
    def _read_from_database(self, from_date: date, to_date: date) -> DataFrame:
        """
        Read data from source database (production implementation).
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with extracted data
        """
        # Example for JDBC connection
        jdbc_url = "jdbc:sap://localhost:30015"
        connection_properties = {
            "user": "SAPABAP",
            "password": "password",
            "driver": "com.sap.db.jdbc.Driver"
        }
        
        query = f"""
            (SELECT * FROM ZSALES_RAW 
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = '{CONSTANTS.status.NEW.value}') as raw_sales
        """
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )
        
        return df