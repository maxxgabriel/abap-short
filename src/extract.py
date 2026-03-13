"""
Extract module for Sales ETL process.
Extracts raw sales data from source using PySpark DataFrame API.
"""
from datetime import datetime
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from src.logger import ETLLogger
from src.schemas import SALES_RAW_SCHEMA


class SalesExtractor:
    """Extracts raw sales data from source table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_table: str = "zsales_raw"
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_table: Source table name
            
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Extract data using Spark SQL
            # In production, this would read from actual source (JDBC, Hive, etc.)
            df = self._read_source_data(source_table, from_date, to_date)
            
            # Validate schema
            if not self._validate_schema(df):
                raise ValueError("Schema validation failed")
            
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
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], SALES_RAW_SCHEMA), False
    
    def _read_source_data(
        self, 
        table_name: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """
        Read data from source table.
        
        Args:
            table_name: Source table name
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data
        """
        # For demonstration, create sample data
        # In production, use: self.spark.read.jdbc() or self.spark.table()
        sample_data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 
             10, 99.99, "USD", "John Doe", "NORTH", "N", 
             datetime.now(), "SYSTEM"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 
             5, 149.99, "USD", "Jane Smith", "SOUTH", "N",
             datetime.now(), "SYSTEM"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 
             20, 99.99, "USD", "John Doe", "EAST", "N",
             datetime.now(), "SYSTEM"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 
             3, 299.99, "USD", "Bob Wilson", "WEST", "N",
             datetime.now(), "SYSTEM"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 
             15, 149.99, "USD", "Jane Smith", "SOUTH", "N",
             datetime.now(), "SYSTEM"),
        ]
        
        df = self.spark.createDataFrame(sample_data, SALES_RAW_SCHEMA)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date) &
            (df.status == "N")
        )
        
        return df
    
    def _validate_schema(self, df: DataFrame) -> bool:
        """
        Validate DataFrame schema matches expected structure.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        expected_fields = {field.name for field in SALES_RAW_SCHEMA.fields}
        actual_fields = {field.name for field in df.schema.fields}
        
        if expected_fields != actual_fields:
            missing = expected_fields - actual_fields
            extra = actual_fields - expected_fields
            self.logger.log_message(
                step="EXTRACT",
                status="W",
                message=f"Schema mismatch. Missing: {missing}, Extra: {extra}"
            )
            return False
        
        return True