"""
Transform module for Sales ETL pipeline.
Handles data transformation and business logic.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Tuple
import logging

from src.logger import ETLLogger


class SalesTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def get_analytics_schema(self) -> StructType:
        """Define schema for analytics data."""
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            total_records = raw_df.count()
            
            # Calculate business metrics
            transformed_df = raw_df.withColumn(
                "gross_amount",
                F.col("quantity") * F.col("unit_price")
            ).withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.config["business_rules"]["discount_qty_tier2"],
                    F.col("gross_amount") * self.config["business_rules"]["discount_rate_tier2"]
                ).when(
                    F.col("quantity") > self.config["business_rules"]["discount_qty_tier1"],
                    F.col("gross_amount") * self.config["business_rules"]["discount_rate_tier1"]
                ).otherwise(F.lit(0.0))
            ).withColumn(
                "tax_amount",
                (F.col("gross_amount") - F.col("discount_amount")) * 
                self.config["business_rules"]["tax_rate"]
            ).withColumn(
                "net_amount",
                F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
            ).withColumn(
                "cost_amount",
                F.col("quantity") * F.col("unit_price") * 
                self.config["business_rules"]["cost_ratio"]
            ).withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
                ).otherwise(F.lit(0.0))
            ).withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.config["business_rules"]["category_high_threshold"],
                    F.lit("HIGH")
                ).when(
                    F.col("gross_amount") >= self.config["business_rules"]["category_medium_threshold"],
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
            ).withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL_"),
                    F.col("trans_id"),
                    F.lit("_"),
                    F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
                )
            ).withColumn(
                "etl_run_id",
                F.lit(self.logger.etl_run_id)
            ).withColumn(
                "loaded_at",
                F.current_timestamp()
            )
            
            # Select final columns
            analytics_df = transformed_df.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                F.col("quantity").alias("total_quantity"),
                "gross_amount",
                "net_amount",
                "discount_amount",
                "tax_amount",
                "currency",
                "sales_rep",
                "region",
                "profit_margin",
                "category",
                "etl_run_id",
                "loaded_at"
            )
            
            # Cache transformed data
            analytics_df.cache()
            success_count = analytics_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=total_records,
                records_success=success_count,
                message=f"Transformed {success_count} of {total_records} records"
            )
            
            return analytics_df, True
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            logging.error(f"Transformation error: {str(e)}", exc_info=True)
            return self.spark.createDataFrame([], self.get_analytics_schema()), False