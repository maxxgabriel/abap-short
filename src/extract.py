"""
Sales Data Extractor Module
Handles extraction of raw sales data from source systems.
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import ExtractionError


class SalesExtractor:
    """
    Extracts raw sales data from source tables.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger instance
        config: Configuration dictionary
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the sales extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
        
        Returns:
            DataFrame containing raw sales data, or None if extraction fails
        
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Define schema for raw sales data
            raw_sales_schema = self._get_raw_sales_schema()
            
            # In production, read from actual source
            # raw_data = self.spark.read \
            #     .format(self.config.get("source_format", "parquet")) \
            #     .schema(raw_sales_schema) \
            #     .load(self.config.get("source_path")) \
            #     .filter((F.col("trans_date") >= from_date) & 
            #             (F.col("trans_date") <= to_date) &
            #             (F.col("status") == "N"))
            
            # For demonstration, create sample data
            raw_data = self._create_sample_data(raw_sales_schema)
            
            count = raw_data.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=count,
                records_success=count,
                message=f"Extracted {count} records successfully"
            )
            
            return raw_data
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=error_msg
            )
            raise ExtractionError(
                error_text=error_msg,
                error_step="EXTRACT"
            ) from e
    
    def _get_raw_sales_schema(self) -> StructType:
        """
        Define the schema for raw sales data.
        
        Returns:
            StructType: Schema definition
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
    
    def _create_sample_data(self, schema: StructType) -> DataFrame:
        """
        Create sample data for demonstration purposes.
        
        Args:
            schema: Schema to use for the DataFrame
        
        Returns:
            DataFrame with sample data
        """
        from datetime import date
        from decimal import Decimal
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema)