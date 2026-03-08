"""
Data extraction module for Sales ETL pipeline.

Extracts raw sales data from source with error handling and validation.
"""

from typing import Optional, List, Tuple
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
import logging

from src.exceptions import ExtractError
from src.logger import ETLLogger


class SalesExtractor:
    """
    Extracts sales data from source systems.
    
    Handles data extraction with comprehensive error handling,
    validation, and logging.
    """
    
    # Source schema definition
    SOURCE_SCHEMA = StructType([
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
    
    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        source_table: str,
        source_format: str = "parquet"
    ):
        """
        Initialize extractor.
        
        Args:
            spark: Active Spark session
            logger: ETL logger instance
            source_table: Source table/path name
            source_format: Data format (parquet, csv, etc.)
        """
        self.spark = spark
        self.logger = logger
        self.source_table = source_table
        self.source_format = source_format
        self._log = logging.getLogger(__name__)
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        status_filter: str = "N"
    ) -> Tuple[DataFrame, int]:
        """
        Extract sales data for date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            status_filter: Status to filter (default 'N' for new)
        
        Returns:
            Tuple of (DataFrame, record_count)
        
        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Validate date range
            self._validate_date_range(from_date, to_date)
            
            # Read source data
            df = self._read_source_data()
            
            # Apply filters
            df_filtered = self._apply_filters(
                df,
                from_date,
                to_date,
                status_filter
            )
            
            # Validate extracted data
            self._validate_extracted_data(df_filtered)
            
            # Count records
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Successfully extracted {record_count} records'
            )
            
            return df_filtered, record_count
            
        except ExtractError:
            raise
        except Exception as e:
            error_msg = f'Extraction failed: {str(e)}'
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=error_msg
            )
            raise ExtractError(
                message=error_msg,
                source_table=self.source_table,
                error_code='EXT_001',
                original_exception=e
            )
    
    def _read_source_data(self) -> DataFrame:
        """
        Read data from source table.
        
        Returns:
            Source DataFrame
        
        Raises:
            ExtractError: If read operation fails
        """
        try:
            self._log.info(f'Reading from source: {self.source_table}')
            
            df = self.spark.read \
                .format(self.source_format) \
                .schema(self.SOURCE_SCHEMA) \
                .load(self.source_table)
            
            return df
            
        except Exception as e:
            raise ExtractError(
                message=f'Failed to read source table: {self.source_table}',
                source_table=self.source_table,
                error_code='EXT_002',
                metadata={'format': self.source_format},
                original_exception=e
            )
    
    def _apply_filters(
        self,
        df: DataFrame,
        from_date: date,
        to_date: date,
        status_filter: str
    ) -> DataFrame:
        """
        Apply filters to source data.
        
        Args:
            df: Source DataFrame
            from_date: Start date
            to_date: End date
            status_filter: Status value
        
        Returns:
            Filtered DataFrame
        """
        from pyspark.sql import functions as F
        
        df_filtered = df.filter(
            (F.col('trans_date') >= F.lit(from_date)) &
            (F.col('trans_date') <= F.lit(to_date)) &
            (F.col('status') == F.lit(status_filter))
        )
        
        return df_filtered
    
    def _validate_date_range(self, from_date: date, to_date: date) -> None:
        """
        Validate date range parameters.
        
        Args:
            from_date: Start date
            to_date: End date
        
        Raises:
            ExtractError: If date range is invalid
        """
        if from_date > to_date:
            raise ExtractError(
                message='From date cannot be later than to date',
                error_code='EXT_003',
                metadata={
                    'from_date': str(from_date),
                    'to_date': str(to_date)
                }
            )
        
        if to_date > date.today():
            raise ExtractError(
                message='To date cannot be in the future',
                error_code='EXT_004',
                metadata={'to_date': str(to_date)}
            )
    
    def _validate_extracted_data(self, df: DataFrame) -> None:
        """
        Validate extracted data quality.
        
        Args:
            df: Extracted DataFrame
        
        Raises:
            ExtractError: If validation fails
        """
        from pyspark.sql import functions as F
        
        # Check for null values in required fields
        required_fields = ['trans_id', 'trans_date', 'customer_id', 
                          'product_id', 'quantity', 'unit_price']
        
        for field in required_fields:
            null_count = df.filter(F.col(field).isNull()).count()
            if null_count > 0:
                raise ExtractError(
                    message=f'Found {null_count} null values in required field: {field}',
                    source_table=self.source_table,
                    error_code='EXT_005',
                    metadata={
                        'field': field,
                        'null_count': null_count
                    }
                )