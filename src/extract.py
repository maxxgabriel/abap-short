"""
Data Extraction Module
Extracts raw sales data from source systems
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from datetime import datetime, timedelta

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import ExtractException


class DataExtractor:
    """Extract raw sales data from source"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize data extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def _get_schema(self) -> StructType:
        """
        Define schema for raw sales data
        
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
    
    def _create_sample_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Create sample data for demonstration
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with sample sales data
        """
        from decimal import Decimal
        
        # Convert dates
        date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        
        sample_data = [
            ("T000001", date_obj, "CUST001", "PROD001", 10, Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", date_obj, "CUST002", "PROD002", 5, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date_obj, "CUST003", "PROD001", 20, Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", date_obj, "CUST001", "PROD003", 3, Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date_obj, "CUST004", "PROD002", 15, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000006", date_obj, "CUST005", "PROD004", 8, Decimal("199.99"), "USD", "Alice Brown", "NORTH", "N"),
            ("T000007", date_obj, "CUST002", "PROD001", 25, Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000008", date_obj, "CUST006", "PROD003", 12, Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self._get_schema())
    
    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data from source
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with raw sales data
            
        Raises:
            ExtractException: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # In production, read from actual source:
            # df = self.spark.read \
            #     .format(self.config.source_format) \
            #     .option("path", self.config.source_path) \
            #     .schema(self._get_schema()) \
            #     .load() \
            #     .filter(f"trans_date BETWEEN '{from_date}' AND '{to_date}'") \
            #     .filter("status = 'N'")
            
            # For demonstration, use sample data
            df = self._create_sample_data(from_date, to_date)
            
            # Apply filters
            df = df.filter(f"trans_date BETWEEN '{from_date}' AND '{to_date}'")
            df = df.filter("status = 'N'")
            
            # Count records
            record_count = df.count()
            
            if record_count == 0:
                self.logger.log_message(
                    step="EXTRACT",
                    status="W",
                    message="No records found for the given date range"
                )
            else:
                self.logger.log_message(
                    step="EXTRACT",
                    status="S",
                    records_processed=record_count,
                    records_success=record_count,
                    message=f"Extracted {record_count} records successfully"
                )
            
            return df
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=error_msg
            )
            raise ExtractException(error_msg)
    
    def validate_prerequisites(self) -> bool:
        """
        Validate extractor prerequisites
        
        Returns:
            True if prerequisites are met
        """
        try:
            # Check if Spark session is active
            if self.spark is None:
                return False
            
            # Check configuration
            if self.config is None:
                return False
            
            return True
            
        except Exception:
            return False