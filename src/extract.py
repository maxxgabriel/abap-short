"""
PySpark ETL Extractor Module

Migrated from ABAP ZCL_ETL_EXTRACTOR class.
Extracts raw sales data from source tables.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import date
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.schemas import RawSalesSchema


class ETLExtractor:
    """
    Extracts raw sales data from source.
    Replaces ABAP SELECT statements with DataFrame reads.
    """
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize extractor with logger and Spark session.
        
        Args:
            logger: ETL logger instance
            spark: Active SparkSession
        """
        self.logger = logger
        self.spark = spark
        self.log = logging.getLogger(__name__)
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_table: str = None,
        source_path: str = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data from source.
        
        Migrated from ABAP extract_data method.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Optional table name (for JDBC)
            source_path: Optional file path (for files)
            
        Returns:
            Tuple of (extracted DataFrame, success boolean)
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Extract data based on source type
            if source_table:
                raw_df = self._extract_from_table(
                    source_table, from_date, to_date
                )
            elif source_path:
                raw_df = self._extract_from_file(
                    source_path, from_date, to_date
                )
            else:
                # Generate sample data for demonstration
                raw_df = self._generate_sample_data()
            
            # Filter by date range and status
            raw_df = raw_df.filter(
                (F.col('trans_date') >= F.lit(from_date)) &
                (F.col('trans_date') <= F.lit(to_date)) &
                (F.col('status') == 'N')
            )
            
            record_count = raw_df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            return raw_df, True
            
        except Exception as e:
            self.log.error(f"Extraction failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            return self.spark.createDataFrame([], RawSalesSchema.get_schema()), False
    
    def _extract_from_table(
        self,
        table_name: str,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Extract data from database table via JDBC.
        
        Args:
            table_name: Name of source table
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with extracted data
        """
        self.log.info(f"Reading from table: {table_name}")
        
        # Read from JDBC source
        # Configuration should be in config.yaml
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/sales_db") \
            .option("dbtable", table_name) \
            .option("user", "etl_user") \
            .option("password", "password") \
            .load()
        
        return df
    
    def _extract_from_file(
        self,
        file_path: str,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Extract data from file (CSV, Parquet, etc.).
        
        Args:
            file_path: Path to source file
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with extracted data
        """
        self.log.info(f"Reading from file: {file_path}")
        
        # Determine format from file extension
        if file_path.endswith('.parquet'):
            df = self.spark.read.parquet(file_path)
        elif file_path.endswith('.csv'):
            df = self.spark.read.csv(
                file_path,
                header=True,
                schema=RawSalesSchema.get_schema()
            )
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        return df
    
    def _generate_sample_data(self) -> DataFrame:
        """
        Generate sample data for demonstration.
        Migrated from ABAP sample data generation.
        
        Returns:
            DataFrame with sample sales data
        """
        self.log.info("Generating sample data")
        
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 
             'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99,
             'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99,
             'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99,
             'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99,
             'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        return self.spark.createDataFrame(
            sample_data,
            schema=RawSalesSchema.get_schema()
        )