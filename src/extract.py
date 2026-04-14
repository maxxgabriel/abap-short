"""
PySpark Data Extraction Module
Extracts raw sales data from source with proper schema mapping and filter conditions.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional
import logging


class SalesDataExtractor:
    """Extracts raw sales data from source tables."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.raw_sales_schema = self._define_raw_sales_schema()
    
    def _define_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data table.
        Corresponds to ZSALES_RAW table structure.
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("created_at", StringType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])
    
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data with date range filter.
        Equivalent to ABAP SELECT with WHERE clause.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            source_path: Optional path to source data (overrides config)
            
        Returns:
            DataFrame containing filtered raw sales data
            
        Raises:
            Exception: If extraction fails
        """
        try:
            self.logger.info(
                f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source path from config or parameter
            path = source_path or self.config.get("source_path")
            source_format = self.config.get("source_format", "parquet")
            
            # Read source data with schema
            df = self.spark.read \
                .format(source_format) \
                .schema(self.raw_sales_schema) \
                .load(path)
            
            # Apply filters (equivalent to ABAP WHERE clause)
            filtered_df = df.filter(
                (F.col("trans_date") >= F.lit(from_date)) &
                (F.col("trans_date") <= F.lit(to_date)) &
                (F.col("status") == F.lit("N"))  # Only new records
            )
            
            # Cache for performance if configured
            if self.config.get("cache_extracted_data", False):
                filtered_df = filtered_df.cache()
            
            record_count = filtered_df.count()
            
            self.logger.info(
                f"Extracted {record_count} records successfully"
            )
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_with_custom_filter(
        self,
        from_date: str,
        to_date: str,
        additional_filters: Optional[str] = None,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data with custom SQL-like filter conditions.
        
        Args:
            from_date: Start date
            to_date: End date
            additional_filters: SQL WHERE clause string
            source_path: Optional source path
            
        Returns:
            Filtered DataFrame
        """
        try:
            # Get base filtered data
            df = self.extract_data(from_date, to_date, source_path)
            
            # Apply additional filters if provided
            if additional_filters:
                df = df.filter(additional_filters)
                self.logger.info(f"Applied additional filters: {additional_filters}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Custom filter extraction failed: {str(e)}")
            raise
    
    def extract_by_region(
        self,
        from_date: str,
        to_date: str,
        regions: list,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data filtered by specific regions.
        
        Args:
            from_date: Start date
            to_date: End date
            regions: List of region codes
            source_path: Optional source path
            
        Returns:
            Region-filtered DataFrame
        """
        try:
            df = self.extract_data(from_date, to_date, source_path)
            
            # Filter by regions
            region_filtered = df.filter(F.col("region").isin(regions))
            
            count = region_filtered.count()
            self.logger.info(
                f"Extracted {count} records for regions: {', '.join(regions)}"
            )
            
            return region_filtered
            
        except Exception as e:
            self.logger.error(f"Region-based extraction failed: {str(e)}")
            raise
    
    def validate_extracted_data(self, df: DataFrame) -> dict:
        """
        Validate extracted data quality.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "total_records": df.count(),
            "null_trans_ids": df.filter(F.col("trans_id").isNull()).count(),
            "null_dates": df.filter(F.col("trans_date").isNull()).count(),
            "invalid_quantities": df.filter(F.col("quantity") <= 0).count(),
            "invalid_prices": df.filter(F.col("unit_price") <= 0).count(),
            "distinct_customers": df.select("customer_id").distinct().count(),
            "distinct_products": df.select("product_id").distinct().count()
        }
        
        self.logger.info(f"Validation results: {validation_results}")
        
        return validation_results
    
    def get_extraction_summary(self, df: DataFrame) -> dict:
        """
        Generate summary statistics for extracted data.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            "total_records": df.count(),
            "date_range": {
                "min_date": df.agg(F.min("trans_date")).collect()[0][0],
                "max_date": df.agg(F.max("trans_date")).collect()[0][0]
            },
            "regions": df.select("region").distinct().count(),
            "sales_reps": df.select("sales_rep").distinct().count(),
            "total_quantity": df.agg(F.sum("quantity")).collect()[0][0],
            "extraction_timestamp": datetime.now().isoformat()
        }
        
        return summary