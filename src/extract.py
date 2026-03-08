"""
ETL Extraction module with integrated logging
"""

from typing import List, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger


class SalesDataExtractor:
    """
    Extract sales data with correlation tracking
    Replaces ZCL_ETL_EXTRACTOR from ABAP
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger with correlation tracking
        """
        self.spark = spark
        self.logger = logger
        
    def get_schema(self) -> StructType:
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
            StructField("status", StringType(), False)
        ])
    
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        source_path: str
    ) -> DataFrame:
        """
        Extract raw sales data
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Source data path
            
        Returns:
            DataFrame with extracted data
        """
        try:
            self.logger.log_extract(
                message=f"Starting extraction from {from_date} to {to_date}",
                source_path=source_path
            )
            
            # Read data with schema
            df = self.spark.read \
                .schema(self.get_schema()) \
                .option("header", "true") \
                .csv(source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            # Add correlation context
            df_with_context = df_filtered.withColumn(
                "correlation_id",
                self.spark.sql.functions.lit(self.logger.get_correlation_id())
            ).withColumn(
                "etl_run_id",
                self.spark.sql.functions.lit(self.logger.get_etl_run_id())
            )
            
            # Cache for performance
            df_with_context.cache()
            record_count = df_with_context.count()
            
            self.logger.log_extract(
                message=f"Extracted {record_count} records successfully",
                records=record_count,
                success=True,
                date_range={'from': from_date, 'to': to_date}
            )
            
            return df_with_context
            
        except Exception as e:
            self.logger.log_error(
                step='EXTRACT',
                message=f"Extraction failed: {str(e)}",
                exception=e,
                source_path=source_path
            )
            raise