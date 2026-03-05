"""
Data Extraction Module
Extracts raw sales data from source database
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from typing import Optional
from datetime import datetime
import logging

from src.logger import ETLLogger
from src.exceptions import ETLExtractError


class SalesDataExtractor:
    """Extracts raw sales data from source systems"""
    
    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the extractor
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self._setup_schema()
    
    def _setup_schema(self) -> None:
        """Define schema for raw sales data"""
        self.raw_sales_schema = StructType([
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
        to_date: str,
        test_mode: bool = False
    ) -> DataFrame:
        """
        Extract raw sales data from source
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            test_mode: If True, return sample data
            
        Returns:
            DataFrame containing raw sales data
            
        Raises:
            ETLExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            if test_mode:
                df = self._generate_sample_data(from_date, to_date)
            else:
                df = self._extract_from_database(from_date, to_date)
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ETLExtractError(f"Failed to extract data: {str(e)}") from e
    
    def _extract_from_database(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from source database
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with extracted data
        """
        db_config = self.config['database']['source']
        
        query = f"""
        (SELECT trans_id, trans_date, customer_id, product_id, 
                quantity, unit_price, currency, sales_rep, region, status
         FROM {db_config['table']}
         WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
         AND status = '{self.config['status_codes']['new']}') AS sales_data
        """
        
        df = self.spark.read \
            .format(db_config['format']) \
            .option("url", db_config['url']) \
            .option("dbtable", query) \
            .option("driver", db_config['driver']) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("fetchsize", db_config['fetch_size']) \
            .schema(self.raw_sales_schema) \
            .load()
        
        return df
    
    def _generate_sample_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Generate sample data for testing
        
        Args:
            from_date: Start date (unused in sample)
            to_date: End date (unused in sample)
            
        Returns:
            DataFrame with sample data
        """
        sample_data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N")
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.raw_sales_schema)
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if df is None or df.count() == 0:
            self.logger.log_message(
                step="EXTRACT",
                status="W",
                message="No data extracted"
            )
            return False
        
        # Check for required columns
        required_columns = [field.name for field in self.raw_sales_schema.fields]
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Missing required columns: {missing_columns}"
            )
            return False
        
        return True