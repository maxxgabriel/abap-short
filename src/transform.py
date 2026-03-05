"""
Data transformation module for Sales ETL Pipeline.
Applies business logic and calculations to raw sales data.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, concat_ws, 
    monotonically_increasing_id, date_format
)
from pyspark.sql.types import DecimalType
from typing import Optional
import logging

from src.logger import ETLLogger


class SalesTransformer:
    """Transforms raw sales data with business logic."""
    
    # Business rules from ABAP constants
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = 0.05
    DISCOUNT_RATE_TIER2 = 0.10
    TAX_RATE = 0.08
    COST_RATIO = 0.60
    CATEGORY_HIGH_THRESHOLD = 2000.00
    CATEGORY_MEDIUM_THRESHOLD = 500.00
    
    def __init__(self, logger: ETLLogger, etl_run_id: str):
        """
        Initialize the transformer.
        
        Args:
            logger: ETL logger instance
            etl_run_id: Unique ETL run identifier
        """
        self.logger = logger
        self.etl_run_id = etl_run_id
    
    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
        
        Returns:
            Transformed analytics DataFrame or None on failure
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Apply business transformations
            transformed_df = self._calculate_analytics(raw_df)
            
            output_count = transformed_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            logging.error(f"Transformation error: {str(e)}", exc_info=True)
            return None
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Apply business calculations and transformations.
        
        Args:
            df: Raw sales DataFrame
        
        Returns:
            Transformed DataFrame with analytics columns
        """
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            (col("quantity") * col("unit_price")).cast(DecimalType(16, 2))
        )
        
        # Calculate discount amount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > self.DISCOUNT_QTY_TIER2, 
                 col("gross_amount") * lit(self.DISCOUNT_RATE_TIER2))
            .when(col("quantity") > self.DISCOUNT_QTY_TIER1,
                  col("gross_amount") * lit(self.DISCOUNT_RATE_TIER1))
            .otherwise(lit(0.0))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate tax amount (8% on gross - discount)
        df = df.withColumn(
            "tax_amount",
            ((col("gross_amount") - col("discount_amount")) * lit(self.TAX_RATE))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            (col("gross_amount") - col("discount_amount") + col("tax_amount"))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate cost and profit margin
        df = df.withColumn(
            "cost_amount",
            (col("quantity") * col("unit_price") * lit(self.COST_RATIO))
            .cast(DecimalType(16, 2))
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 (((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100)))
            .otherwise(lit(0.0))
            .cast(DecimalType(5, 2))
        )
        
        # Categorize sales based on gross amount
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= self.CATEGORY_HIGH_THRESHOLD, lit("HIGH"))
            .when(col("gross_amount") >= self.CATEGORY_MEDIUM_THRESHOLD, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat_ws("_", 
                lit("ANL"),
                col("trans_id"),
                date_format(current_timestamp(), "yyyyMMddHHmmss"),
                monotonically_increasing_id()
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.etl_run_id))
        df = df.withColumn("loaded_at", current_timestamp())
        df = df.withColumn("loaded_by", lit("PYSPARK_ETL"))
        
        # Select and rename columns for analytics output
        analytics_df = df.select(
            col("analytics_id"),
            col("trans_date"),
            col("customer_id"),
            col("product_id"),
            col("quantity").alias("total_quantity"),
            col("gross_amount"),
            col("net_amount"),
            col("discount_amount"),
            col("tax_amount"),
            col("currency"),
            col("sales_rep"),
            col("region"),
            col("profit_margin"),
            col("category"),
            col("etl_run_id"),
            col("loaded_at"),
            col("loaded_by")
        )
        
        return analytics_df
    
    def validate_transformed_data(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed data and filter out invalid records.
        
        Args:
            df: Transformed DataFrame
        
        Returns:
            DataFrame with only valid records
        """
        valid_df = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0) &
            col("currency").isNotNull() &
            col("category").isin(["HIGH", "MEDIUM", "LOW"])
        )
        
        invalid_count = df.count() - valid_df.count()
        if invalid_count > 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="W",
                message=f"Filtered out {invalid_count} invalid records"
            )
        
        return valid_df