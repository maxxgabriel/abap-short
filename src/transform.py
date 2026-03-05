"""
Data Transformer - transforms raw sales data into analytics format
Migrated from ABAP ZCL_ETL_TRANSFORMER
"""

import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, date_format, current_timestamp, udf
)
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType

from src.config import Config
from src.etl_logger import ETLLogger


class Transformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, config: Config, etl_logger: ETLLogger):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            etl_logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.etl_logger = etl_logger
        self.logger = logging.getLogger(__name__)
    
    def get_analytics_schema(self) -> StructType:
        """Get schema for analytics data."""
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=False),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=False),
            StructField("discount_amount", DecimalType(16, 2), nullable=False),
            StructField("tax_amount", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=False),
        ])
    
    def transform_data(self, raw_data: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_data: DataFrame with raw sales data
            
        Returns:
            DataFrame with transformed analytics data
        """
        try:
            self.etl_logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            self.logger.info("Starting data transformation")
            
            initial_count = raw_data.count()
            
            # Calculate gross amount
            df = raw_data.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(
                    col("quantity") > self.config.business_rules["discount_qty_tier2"],
                    col("gross_amount") * self.config.business_rules["discount_rate_tier2"]
                ).when(
                    col("quantity") > self.config.business_rules["discount_qty_tier1"],
                    col("gross_amount") * self.config.business_rules["discount_rate_tier1"]
                ).otherwise(lit(0.0))
            )
            
            # Calculate tax (on gross - discount)
            df = df.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * self.config.business_rules["tax_rate"]
            )
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Calculate profit margin (simplified: assume cost is 60% of unit price)
            df = df.withColumn(
                "cost_amount",
                col("quantity") * col("unit_price") * self.config.business_rules["cost_ratio"]
            )
            
            df = df.withColumn(
                "profit_margin",
                when(
                    col("net_amount") > 0,
                    ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100
                ).otherwise(lit(0.0))
            )
            
            # Categorize sales
            df = df.withColumn(
                "category",
                when(
                    col("gross_amount") >= self.config.business_rules["category_high_threshold"],
                    lit("HIGH")
                ).when(
                    col("gross_amount") >= self.config.business_rules["category_medium_threshold"],
                    lit("MEDIUM")
                ).otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            df = df.withColumn(
                "analytics_id",
                concat(
                    lit("ANL"),
                    col("trans_id"),
                    date_format(current_timestamp(), "HHmmss")
                )
            )
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", lit(self.etl_logger.etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            
            # Select and rename columns to match analytics schema
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
                col("loaded_at")
            )
            
            # Cache transformed data
            analytics_df.cache()
            
            final_count = analytics_df.count()
            
            self.etl_logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=initial_count,
                records_success=final_count,
                records_error=initial_count - final_count,
                message=f'Transformed {final_count} of {initial_count} records'
            )
            
            self.logger.info(f"Transformed {final_count} of {initial_count} records")
            
            return analytics_df
            
        except Exception as e:
            self.etl_logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise