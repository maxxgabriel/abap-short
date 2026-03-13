"""
ETL Extract Module
Extracts raw sales data with logging integration
"""

from typing import List, Dict, Any, Optional
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger, ETLStep, ETLStatus


class SalesDataExtractor:
    """Extract raw sales data from source"""

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize extractor

        Args:
            logger: ETL logger instance
            spark: SparkSession instance
        """
        self.logger = logger
        self.spark = spark

    def get_schema(self) -> StructType:
        """Define schema for raw sales data"""
        return StructType([
            StructField('trans_id', StringType(), False),
            StructField('trans_date', DateType(), False),
            StructField('customer_id', StringType(), False),
            StructField('product_id', StringType(), False),
            StructField('quantity', IntegerType(), False),
            StructField('unit_price', DecimalType(16, 2), False),
            StructField('currency', StringType(), False),
            StructField('sales_rep', StringType(), True),
            StructField('region', StringType(), True),
            StructField('status', StringType(), False)
        ])

    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract sales data for date range

        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Optional path to source data

        Returns:
            DataFrame with extracted data

        Raises:
            Exception: If extraction fails
        """
        try:
            self.logger.log_info(
                ETLStep.EXTRACT,
                f"Starting extraction from {from_date} to {to_date}",
                from_date=str(from_date),
                to_date=str(to_date)
            )

            if source_path:
                # Read from file source
                df = self.spark.read.schema(self.get_schema()).parquet(source_path)

                # Filter by date range and status
                df = df.filter(
                    (df.trans_date >= from_date) &
                    (df.trans_date <= to_date) &
                    (df.status == 'N')
                )
            else:
                # Create sample data for demonstration
                df = self._create_sample_data()

            count = df.count()

            self.logger.log_success(
                ETLStep.EXTRACT,
                f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count,
                records_error=0
            )

            return df

        except Exception as e:
            self.logger.log_error(
                ETLStep.EXTRACT,
                f"Extraction failed: {str(e)}",
                exception=e
            )
            raise

    def _create_sample_data(self) -> DataFrame:
        """Create sample data for testing"""
        from datetime import datetime

        sample_data = [
            {
                'trans_id': 'T000001',
                'trans_date': datetime.now().date(),
                'customer_id': 'CUST001',
                'product_id': 'PROD001',
                'quantity': 10,
                'unit_price': 99.99,
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
                'unit_price': 149.99,
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
                'unit_price': 99.99,
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
                'unit_price': 299.99,
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
                'unit_price': 149.99,
                'currency': 'USD',
                'sales_rep': 'Jane Smith',
                'region': 'SOUTH',
                'status': 'N'
            }
        ]

        return self.spark.createDataFrame(sample_data, schema=self.get_schema())