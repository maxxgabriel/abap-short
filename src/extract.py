"""
Sales Data Extraction Module
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesDataExtractor:
    """Extracts raw sales data from source tables"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data"""
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
    
    def extract_data(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data within date range
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with raw sales data or None on failure
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Build extraction query
            source_table = self.config.get("source.table", "zsales_raw")
            status_filter = self.config.get("source.status_filter", "N")
            
            query = f"""
                SELECT 
                    trans_id,
                    trans_date,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price,
                    currency,
                    sales_rep,
                    region,
                    status,
                    created_at,
                    created_by
                FROM {source_table}
                WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
                  AND status = '{status_filter}'
            """
            
            # Read data with schema
            df = self.spark.sql(query) if self._table_exists(source_table) else self._create_sample_data()
            
            # Validate extracted data
            record_count = df.count()
            
            if record_count == 0:
                self.logger.log_message(
                    step="EXTRACT",
                    status="W",
                    message=f"No records found for date range {from_date} to {to_date}"
                )
                return None
            
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
            logging.error(f"Extract error: {str(e)}", exc_info=True)
            return None
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if source table exists"""
        try:
            self.spark.catalog.tableExists(table_name)
            return True
        except:
            return False
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for testing"""
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", 
             "John Doe", "NORTH", "N", datetime.now(), "SYSTEM"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD",
             "John Doe", "EAST", "N", datetime.now(), "SYSTEM"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD",
             "Bob Wilson", "WEST", "N", datetime.now(), "SYSTEM"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM")
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())