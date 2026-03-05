"""
ETL Extractor Module
Extracts raw sales data from source using PySpark.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Optional
import yaml

from src.utils.etl_logger import ETLLogger


class ETLExtractor:
    """
    Extracts raw sales data from source table.
    Converted from ZCL_ETL_EXTRACTOR ABAP class.
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
        self.raw_sales_schema = self._get_raw_sales_schema()
    
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
            StructField("status", StringType(), False)
        ])
    
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        source_table: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_table: Optional source table name
            
        Returns:
            DataFrame with extracted data
            
        Raises:
            Exception: If extraction fails
        """
        step = self.config['process_steps']['extract']
        
        try:
            self.logger.log_etl_message(
                step=step,
                status=self.config['status_codes']['success'],
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # In production, read from actual table:
            # df = self.spark.table(source_table or "zsales_raw")
            # df = df.filter(
            #     (df.trans_date >= from_date) & 
            #     (df.trans_date <= to_date) &
            #     (df.status == self.config['status_codes']['new'])
            # )
            
            # For demonstration, create sample data
            sample_data = [
                ("T000001", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST001", "PROD001", 
                 10, 99.99, "USD", "John Doe", "NORTH", "N"),
                ("T000002", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST002", "PROD002", 
                 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
                ("T000003", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST003", "PROD001", 
                 20, 99.99, "USD", "John Doe", "EAST", "N"),
                ("T000004", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST001", "PROD003", 
                 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
                ("T000005", datetime.strptime("2024-01-15", "%Y-%m-%d").date(), "CUST004", "PROD002", 
                 15, 149.99, "USD", "Jane Smith", "SOUTH", "N")
            ]
            
            df = self.spark.createDataFrame(sample_data, self.raw_sales_schema)
            
            record_count = df.count()
            
            self.logger.log_etl_statistics(
                step=step,
                status=self.config['status_codes']['success'],
                records_processed=record_count,
                records_success=record_count,
                records_error=0,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_etl_message(
                step=step,
                status=self.config['status_codes']['error'],
                message=f"Extraction failed: {str(e)}"
            )
            raise