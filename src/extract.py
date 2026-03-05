"""
ETL Extractor Module
Migrated from ZCL_ETL_EXTRACTOR ABAP class
Extracts raw sales data from source
"""
from typing import Tuple, Optional
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLExtractor:
    """
    ETL Extractor class for data extraction
    Migrated from ABAP ZCL_ETL_EXTRACTOR
    """
    
    # Schema for raw sales data
    RAW_SALES_SCHEMA = StructType([
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
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize ETL Extractor
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_table: str = "zsales_raw"
    ) -> Tuple[bool, Optional[DataFrame]]:
        """
        Extract raw sales data from source
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name
            
        Returns:
            Tuple of (success flag, DataFrame with extracted data)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.STEPS.EXTRACT,
                status=ETLConstants.STATUS.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Extract data from source table
            # In production, replace with actual table read
            sales_df = self._extract_from_source(from_date, to_date, source_table)
            
            if sales_df is None or sales_df.count() == 0:
                self.logger.log_message(
                    step=ETLConstants.STEPS.EXTRACT,
                    status=ETLConstants.STATUS.WARNING,
                    message="No data found for the specified date range"
                )
                return False, None
            
            record_count = sales_df.count()
            
            self.logger.log_message(
                step=ETLConstants.STEPS.EXTRACT,
                status=ETLConstants.STATUS.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return True, sales_df
            
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.STEPS.EXTRACT,
                status=ETLConstants.STATUS.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return False, None
    
    def _extract_from_source(
        self,
        from_date: date,
        to_date: date,
        source_table: str
    ) -> Optional[DataFrame]:
        """
        Extract data from source table
        
        Args:
            from_date: Start date
            to_date: End date
            source_table: Source table name
            
        Returns:
            DataFrame with extracted data
        """
        try:
            # In production environment, use actual table read:
            # df = self.spark.table(source_table)
            # df = df.filter(
            #     (df.trans_date >= from_date) & 
            #     (df.trans_date <= to_date) &
            #     (df.status == ETLConstants.STATUS.NEW)
            # )
            # return df
            
            # For demonstration, create sample data
            return self._create_sample_data()
            
        except Exception as e:
            self.logger.logger.error(f"Failed to extract from {source_table}: {str(e)}")
            raise
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample data for demonstration
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import datetime
        from decimal import Decimal
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.RAW_SALES_SCHEMA)