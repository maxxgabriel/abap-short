"""
ETL Transform Module - Sales Data Transformation
Transforms raw sales data into analytics format with business logic
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.config import Config


class SalesTransformer:
    """Transform raw sales data into analytics format"""
    
    def __init__(self, logger: ETLLogger, config: Config):
        """
        Initialize transformer
        
        Args:
            logger: ETL logger instance
            config: Configuration object
        """
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data
        
        Returns:
            StructType schema for analytics
        """
        return StructType([
            StructField("analytics_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("total_quantity", IntegerType(), False),
            StructField("gross_amount", DecimalType(16, 2), False),
            StructField("net_amount", DecimalType(16, 2), False),
            StructField("discount_amount", DecimalType(16, 2), False),
            StructField("tax_amount", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), True),
            StructField("region", StringType(), True),
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (Transformed DataFrame, success flag)
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Calculate gross amount
            transformed_df = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            
            # Calculate discount based on quantity tiers
            transformed_df = transformed_df.withColumn(
                "discount_amount",
                when(col("quantity") > self.config.discount_qty_tier2, 
                     col("gross_amount") * self.config.discount_rate_tier2)
                .when(col("quantity") > self.config.discount_qty_tier1,
                      col("gross_amount") * self.config.discount_rate_tier1)
                .otherwise(lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            transformed_df = transformed_df.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * self.config.tax_rate
            )
            
            # Calculate net amount
            transformed_df = transformed_df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Calculate cost and profit margin
            transformed_df = transformed_df.withColumn(
                "cost",
                col("quantity") * col("unit_price") * self.config.cost_ratio
            )
            
            transformed_df = transformed_df.withColumn(
                "profit_margin",
                spark_round(
                    ((col("net_amount") - col("cost")) / col("net_amount")) * 100, 
                    2
                )
            )
            
            # Categorize sales
            transformed_df = transformed_df.withColumn(
                "category",
                when(col("gross_amount") >= self.config.category_high_threshold, lit("HIGH"))
                .when(col("gross_amount") >= self.config.category_medium_threshold, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID and add ETL run ID
            from pyspark.sql.functions import concat, col as F_col, monotonically_increasing_id
            
            transformed_df = transformed_df.withColumn(
                "analytics_id",
                concat(lit("ANL"), F_col("trans_id"), 
                       monotonically_increasing_id().cast(StringType()))
            )
            
            transformed_df = transformed_df.withColumn(
                "etl_run_id",
                lit(self.logger.get_etl_run_id())
            )
            
            # Select and rename columns to match analytics schema
            analytics_df = transformed_df.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                col("quantity").alias("total_quantity"),
                "gross_amount",
                "net_amount",
                "discount_amount",
                "tax_amount",
                "currency",
                "sales_rep",
                "region",
                "profit_margin",
                "category",
                "etl_run_id"
            )
            
            final_count = analytics_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                records_error=initial_count - final_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return analytics_df, True
            
        except Exception as e:
            self.log.error(f"Transformation failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            return raw_df.limit(0), False