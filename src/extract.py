"""
Data Extraction Module
Extracts raw sales data from source with date range filtering.
"""

from typing import Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime

from src.logger import ETLLogger


class Extractor:
    """
    Handles data extraction from raw sales source.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
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
    
    def extract_data(
        self,
        from_date: str,
        to_date: str
    ) -> Tuple[bool, Optional[DataFrame]]:
        """
        Extract data from source within date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            Tuple of (success flag, DataFrame or None)
        """
        try:
            self.logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Create sample data for demonstration
            raw_data = self._create_sample_data(from_date, to_date)
            
            # In production, would read from actual source:
            # data_source_config = self.config.get("data_sources", {}).get("raw_sales", {})
            # raw_data = self.spark.read \
            #     .format(data_source_config.get("format", "parquet")) \
            #     .schema(self.get_raw_sales_schema()) \
            #     .load(data_source_config.get("path")) \
            #     .filter(f"trans_date BETWEEN '{from_date}' AND '{to_date}'") \
            #     .filter("status = 'N'")
            
            count = raw_data.count()
            
            self.logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                records_processed=count,
                records_success=count,
                message=f"Extracted {count} records successfully"
            )
            
            return True, raw_data
            
        except Exception as e:
            self.logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return False, None
    
    def _create_sample_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Create sample data for demonstration.
        
        Args:
            from_date: Start date
            to_date: End date
        
        Returns:
            Sample DataFrame
        """
        from decimal import Decimal
        
        sample_data = [
            ("T000001", datetime.strptime(from_date, "%Y-%m-%d").date(), 
             "CUST001", "PROD001", 10, Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", datetime.strptime(from_date, "%Y-%m-%d").date(),
             "CUST002", "PROD002", 5, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", datetime.strptime(from_date, "%Y-%m-%d").date(),
             "CUST003", "PROD001", 20, Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", datetime.strptime(from_date, "%Y-%m-%d").date(),
             "CUST001", "PROD003", 3, Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", datetime.strptime(from_date, "%Y-%m-%d").date(),
             "CUST004", "PROD002", 15, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N")
        ]
        
        return self.spark.createDataFrame(
            sample_data,
            schema=self.get_raw_sales_schema()
        )