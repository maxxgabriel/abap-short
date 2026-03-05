"""
Data extraction module for ETL system.

This module handles extraction of raw sales data from source systems.
Replaces ABAP class: ZCL_ETL_EXTRACTOR
"""

from typing import Optional, Tuple
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import logging

from src.exceptions import ExtractError
from src.logger import ETLLogger


class SalesExtractor:
    """
    Extracts raw sales data from source systems.
    
    This class handles the EXTRACT phase of the ETL process, reading
    sales transactions from source tables/files and applying initial
    data quality checks.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger for tracking operations
        schema: Expected schema for raw sales data
    """
    
    # Schema definition for raw sales data
    RAW_SALES_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor with Spark session and logger.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.schema = self.RAW_SALES_SCHEMA
        
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: Optional[str] = None,
        source_table: Optional[str] = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract sales data for specified date range.
        
        Args:
            from_date: Start date for extraction (inclusive)
            to_date: End date for extraction (inclusive)
            source_path: Optional file path for CSV/Parquet source
            source_table: Optional table name for database source
            
        Returns:
            Tuple of (DataFrame with extracted data, success boolean)
            
        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Extract from configured source
            if source_path:
                df = self._extract_from_file(source_path)
            elif source_table:
                df = self._extract_from_table(source_table)
            else:
                # For demo: create sample data
                df = self._create_sample_data()
            
            # Filter by date range
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
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
            
            return df_filtered, True
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(
                message=f"Data extraction failed: {str(e)}",
                context={
                    "from_date": str(from_date),
                    "to_date": str(to_date),
                    "source_path": source_path,
                    "source_table": source_table
                }
            )
    
    def _extract_from_file(self, file_path: str) -> DataFrame:
        """
        Extract data from file (CSV, Parquet, etc.).
        
        Args:
            file_path: Path to source file
            
        Returns:
            DataFrame with extracted data
        """
        if file_path.endswith('.csv'):
            return self.spark.read.csv(
                file_path,
                schema=self.schema,
                header=True
            )
        elif file_path.endswith('.parquet'):
            return self.spark.read.parquet(file_path)
        else:
            raise ExtractError(
                message=f"Unsupported file format: {file_path}",
                context={"file_path": file_path}
            )
    
    def _extract_from_table(self, table_name: str) -> DataFrame:
        """
        Extract data from database table.
        
        Args:
            table_name: Name of source table
            
        Returns:
            DataFrame with extracted data
        """
        return self.spark.table(table_name)
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample data for demonstration/testing.
        
        Returns:
            DataFrame with sample sales records
        """
        from datetime import datetime
        
        sample_data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.schema)