"""
Data extraction module for Sales ETL.
Extracts raw sales data from source table.
Migrated from ABAP ZCL_ETL_EXTRACTOR class.
"""

from datetime import date
from typing import Optional
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.config import ProcessStep, StatusCode


class SalesExtractor:
    """Extracts raw sales data from source system."""
    
    # Schema for raw sales data
    RAW_SALES_SCHEMA = StructType([
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
        from_date: date,
        to_date: date,
        source_table: str = "zsales_raw"
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data from source table.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name
            
        Returns:
            DataFrame with extracted data, or None if extraction fails
        """
        try:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=StatusCode.INFO,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Build query for extraction
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
                    status
                FROM {source_table}
                WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
                  AND status = 'N'
            """
            
            # Execute extraction
            # Note: In production, replace with actual database read
            df = self._create_sample_data()  # Demo data
            
            # Validate schema
            df = self.spark.createDataFrame(df.rdd, schema=self.RAW_SALES_SCHEMA)
            
            record_count = df.count()
            
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=StatusCode.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=StatusCode.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return None
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample data for demonstration.
        In production, this would be replaced with actual database read.
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import datetime
        
        sample_data = [
            {
                'trans_id': 'T000001',
                'trans_date': datetime.now().date(),
                'customer_id': 'CUST001',
                'product_id': 'PROD001',
                'quantity': 10,
                'unit_price': Decimal('99.99'),
                'currency': 'USD',
                'sales_rep': 'John Doe',
                'region': 'NORTH',
                'status': 'N'
            },
            {
                'trans_id': 'T000002',
                'trans_date': datetime.now().date(),
                'customer_id': 'CUST002',
                'product_id': 'PROD002',
                'quantity': 5,
                'unit_price': Decimal('149.99'),
                'currency': 'USD',
                'sales_rep': 'Jane Smith',
                'region': 'SOUTH',
                'status': 'N'
            },
            {
                'trans_id': 'T000003',
                'trans_date': datetime.now().date(),
                'customer_id': 'CUST003',
                'product_id': 'PROD001',
                'quantity': 20,
                'unit_price': Decimal('99.99'),
                'currency': 'USD',
                'sales_rep': 'John Doe',
                'region': 'EAST',
                'status': 'N'
            },
            {
                'trans_id': 'T000004',
                'trans_date': datetime.now().date(),
                'customer_id': 'CUST001',
                'product_id': 'PROD003',
                'quantity': 3,
                'unit_price': Decimal('299.99'),
                'currency': 'USD',
                'sales_rep': 'Bob Wilson',
                'region': 'WEST',
                'status': 'N'
            },
            {
                'trans_id': 'T000005',
                'trans_date': datetime.now().date(),
                'customer_id': 'CUST004',
                'product_id': 'PROD002',
                'quantity': 15,
                'unit_price': Decimal('149.99'),
                'currency': 'USD',
                'sales_rep': 'Jane Smith',
                'region': 'SOUTH',
                'status': 'N'
            }
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.RAW_SALES_SCHEMA)