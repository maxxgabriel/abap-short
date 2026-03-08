"""
Extract module for Sales ETL process.
Extracts raw sales data from source table.
"""

import logging
from datetime import datetime
from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)

from src.logger import ETLLogger
from src.schemas import RawSalesSchema


logger = logging.getLogger(__name__)


class Extractor:
    """Handles data extraction from raw sales source."""
    
    def __init__(self, spark: SparkSession, etl_logger: ETLLogger, config: dict):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            etl_logger: ETL logging instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_logger = etl_logger
        self.config = config
        
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_table: str = None
    ) -> Tuple[bool, DataFrame]:
        """
        Extract raw sales data for date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_table: Optional override for source table name
            
        Returns:
            Tuple of (success flag, extracted DataFrame)
        """
        step = "EXTRACT"
        
        try:
            self.etl_logger.log_message(
                step=step,
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get table name from config or parameter
            table_name = source_table or self.config.get("source_table", "zsales_raw")
            
            # Build query with date filter and status
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
                FROM {table_name}
                WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
                AND status = 'N'
            """
            
            # Execute extraction
            df = self.spark.sql(query)
            
            # Cache for multiple operations
            df.cache()
            
            record_count = df.count()
            
            # Validate extraction
            if record_count == 0:
                self.etl_logger.log_message(
                    step=step,
                    status="W",
                    records_processed=0,
                    records_success=0,
                    message="No records found for extraction criteria"
                )
                return True, df
            
            self.etl_logger.log_message(
                step=step,
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            logger.info(f"Extracted {record_count} records from {table_name}")
            
            return True, df
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.etl_logger.log_message(
                step=step,
                status="E",
                message=error_msg
            )
            logger.error(error_msg, exc_info=True)
            return False, self.spark.createDataFrame([], RawSalesSchema.get_schema())
            
    def extract_from_source(self, source_path: str, format: str = "parquet") -> Tuple[bool, DataFrame]:
        """
        Extract data from file source.
        
        Args:
            source_path: Path to source data
            format: Data format (parquet, csv, json)
            
        Returns:
            Tuple of (success flag, extracted DataFrame)
        """
        step = "EXTRACT"
        
        try:
            self.etl_logger.log_message(
                step=step,
                status="S",
                message=f"Starting extraction from {source_path} ({format})"
            )
            
            # Read based on format
            if format.lower() == "csv":
                df = self.spark.read.csv(
                    source_path,
                    schema=RawSalesSchema.get_schema(),
                    header=True
                )
            elif format.lower() == "json":
                df = self.spark.read.json(
                    source_path,
                    schema=RawSalesSchema.get_schema()
                )
            else:  # Default to parquet
                df = self.spark.read.parquet(
                    source_path,
                    schema=RawSalesSchema.get_schema()
                )
            
            df.cache()
            record_count = df.count()
            
            self.etl_logger.log_message(
                step=step,
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records from {source_path}"
            )
            
            return True, df
            
        except Exception as e:
            error_msg = f"File extraction failed: {str(e)}"
            self.etl_logger.log_message(
                step=step,
                status="E",
                message=error_msg
            )
            logger.error(error_msg, exc_info=True)
            return False, self.spark.createDataFrame([], RawSalesSchema.get_schema())
            
    def validate_extracted_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate extracted data quality.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation messages)
        """
        validation_messages = []
        is_valid = True
        
        # Check for null values in key columns
        key_columns = ["trans_id", "customer_id", "product_id", "quantity", "unit_price"]
        
        for col in key_columns:
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                validation_messages.append(f"Found {null_count} null values in {col}")
                is_valid = False
                
        # Check for negative quantities or prices
        negative_qty = df.filter(df["quantity"] <= 0).count()
        if negative_qty > 0:
            validation_messages.append(f"Found {negative_qty} records with negative/zero quantity")
            is_valid = False
            
        negative_price = df.filter(df["unit_price"] <= 0).count()
        if negative_price > 0:
            validation_messages.append(f"Found {negative_price} records with negative/zero price")
            is_valid = False
            
        if is_valid:
            validation_messages.append("All validation checks passed")
            
        return is_valid, validation_messages