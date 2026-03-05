"""
PySpark Data Extractor Module
Extracts raw sales data from source with date filtering using dependency injection pattern.
"""

from datetime import date
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType
)
from src.logger import ETLLogger


class SalesDataExtractor:
    """
    Extracts raw sales data with date-based filtering.
    Uses dependency injection for logger to enable testability.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor with Spark session and logger.
        
        Args:
            spark: Active SparkSession instance
            logger: ETLLogger instance for tracking operations
        """
        self.spark = spark
        self.logger = logger
        self._schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema matching source data structure
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
    
    def extract_data(
        self, 
        from_date: date, 
        to_date: date,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract sales data filtered by date range.
        
        Args:
            from_date: Start date for extraction (inclusive)
            to_date: End date for extraction (inclusive)
            source_path: Optional path to source data. If None, uses config default.
            
        Returns:
            DataFrame containing filtered raw sales data
            
        Raises:
            ValueError: If date range is invalid
            RuntimeError: If extraction fails
        """
        # Validate date range
        if from_date > to_date:
            error_msg = f"Invalid date range: from_date ({from_date}) > to_date ({to_date})"
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=error_msg
            )
            raise ValueError(error_msg)
        
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read source data
            if source_path:
                df = self.spark.read.schema(self._schema).parquet(source_path)
            else:
                # For demo: create sample data
                df = self._create_sample_data()
            
            # Apply date filter
            filtered_df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            # Cache for performance
            filtered_df.cache()
            record_count = filtered_df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return filtered_df
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=error_msg
            )
            raise RuntimeError(error_msg) from e
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample data for demonstration purposes.
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import datetime
        
        sample_data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self._schema)
    
    def get_schema(self) -> StructType:
        """
        Get the schema used for extraction.
        
        Returns:
            StructType schema definition
        """
        return self._schema