"""
Data Extraction Component
Migrated from ZCL_ETL_EXTRACTOR
"""

from datetime import date
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

from src.logger import ETLLogger
from src.constants import ETLConstants
from src.types import ETLSchemas


class ETLExtractor:
    """
    Extract raw sales data from source
    Migrated from ZCL_ETL_EXTRACTOR
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.constants = ETLConstants()
        
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_table: Optional[str] = None
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data for date range
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name (optional, uses config if not provided)
            
        Returns:
            DataFrame with raw sales data or None on error
        """
        try:
            self.logger.log_message(
                step=ETLConstants.STEP.EXTRACT,
                status=ETLConstants.STATUS.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source table name from config if not provided
            if source_table is None:
                source_table = self.constants.get('database.source.table_name', 'zsales_raw')
            
            # Extract data from source table
            sales_df = self._read_source_data(source_table, from_date, to_date)
            
            if sales_df is None:
                raise Exception("Failed to read source data")
            
            record_count = sales_df.count()
            
            self.logger.log_message(
                step=ETLConstants.STEP.EXTRACT,
                status=ETLConstants.STATUS.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return sales_df
            
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.STEP.EXTRACT,
                status=ETLConstants.STATUS.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return None
            
    def _read_source_data(
        self,
        table_name: str,
        from_date: date,
        to_date: date
    ) -> Optional[DataFrame]:
        """
        Read data from source table
        
        Args:
            table_name: Source table name
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame or None
        """
        try:
            # In production, read from actual table
            # df = self.spark.table(table_name)
            # filtered_df = df.filter(
            #     (col('trans_date') >= lit(from_date)) &
            #     (col('trans_date') <= lit(to_date)) &
            #     (col('status') == ETLConstants.STATUS.NEW)
            # )
            
            # For demonstration, create sample data
            filtered_df = self._create_sample_data(from_date, to_date)
            
            return filtered_df
            
        except Exception as e:
            self.logger.logger.error(f"Error reading source data: {e}")
            return None
            
    def _create_sample_data(self, from_date: date, to_date: date) -> DataFrame:
        """Create sample data for demonstration"""
        sample_data = [
            ('T000001', from_date, 'CUST001', 'PROD001', 10, 99.99, 'USD', 
             'John Doe', 'NORTH', 'N', None, None),
            ('T000002', from_date, 'CUST002', 'PROD002', 5, 149.99, 'USD',
             'Jane Smith', 'SOUTH', 'N', None, None),
            ('T000003', from_date, 'CUST003', 'PROD001', 20, 99.99, 'USD',
             'John Doe', 'EAST', 'N', None, None),
            ('T000004', from_date, 'CUST001', 'PROD003', 3, 299.99, 'USD',
             'Bob Wilson', 'WEST', 'N', None, None),
            ('T000005', from_date, 'CUST004', 'PROD002', 15, 149.99, 'USD',
             'Jane Smith', 'SOUTH', 'N', None, None),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=ETLSchemas.raw_sales_schema())
        df = df.withColumn('created_at', current_timestamp())
        df = df.withColumn('created_by', lit('SYSTEM'))
        
        return df