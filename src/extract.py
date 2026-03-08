"""
ETL Extractor Module
Extracts raw sales data from source table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Optional
import logging

from src.logger import ETLLogger
from src.exceptions import ETLExtractError


class ETLExtractor:
    """Extracts raw sales data from source table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the ETL Extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def extract_data(
        self,
        source_table: str,
        from_date: str,
        to_date: str,
        status_filter: str = 'N'
    ) -> DataFrame:
        """
        Extract raw sales data from source table.
        
        Args:
            source_table: Source table name
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            status_filter: Status to filter (default 'N' for new records)
            
        Returns:
            DataFrame containing extracted raw sales data
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Extract data with filters
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
                AND status = '{status_filter}'
            """
            
            df = self.spark.sql(query)
            
            # Count extracted records
            record_count = df.count()
            
            if record_count == 0:
                self.logger.log_message(
                    step='EXTRACT',
                    status='W',
                    message='No records found for extraction',
                    records_processed=0
                )
            else:
                self.logger.log_message(
                    step='EXTRACT',
                    status='S',
                    message=f'Extracted {record_count} records successfully',
                    records_processed=record_count,
                    records_success=record_count
                )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise ETLExtractError(f'Data extraction failed: {str(e)}')