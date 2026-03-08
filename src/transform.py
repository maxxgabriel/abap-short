"""
Sales Data Transformer Module
Transforms raw sales data into analytics format with business logic.
"""
from typing import Optional
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import TransformationError


class SalesTransformer:
    """
    Transformer component for sales data.
    Applies business rules and calculations to generate analytics data.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer with Spark session and logger.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Load business rules from config
        self.business_rules = config.get('business_rules', {})
        self.discount_qty_tier1 = self.business_rules.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = self.business_rules.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = self.business_rules.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = self.business_rules.get('discount_rate_tier2', 0.10)
        self.tax_rate = self.business_rules.get('tax_rate', 0.08)
        self.cost_ratio = self.business_rules.get('cost_ratio', 0.60)
        self.category_high_threshold = self.business_rules.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = self.business_rules.get('category_medium_threshold', 500.00)
    
    @staticmethod
    def get_analytics_schema() -> StructType:
        """
        Define schema for analytics data.
        
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
    
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            logging.info("Starting data transformation")
            
            input_count = raw_df.count()
            
            # Generate analytics ID
            analytics_df = raw_df.withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL_"),
                    F.col("trans_id"),
                    F.lit("_"),
                    F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
                )
            )
            
            # Calculate gross amount
            analytics_df = analytics_df.withColumn(
                "gross_amount",
                (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
            )
            
            # Calculate discount based on quantity tiers
            analytics_df = analytics_df.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_qty_tier2,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier2)
                ).when(
                    F.col("quantity") > self.discount_qty_tier1,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier1)
                ).otherwise(F.lit(0.0)).cast(DecimalType(16, 2))
            )
            
            # Calculate tax amount (on gross - discount)
            analytics_df = analytics_df.withColumn(
                "tax_amount",
                ((F.col("gross_amount") - F.col("discount_amount")) * F.lit(self.tax_rate))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate net amount
            analytics_df = analytics_df.withColumn(
                "net_amount",
                (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate cost and profit margin
            analytics_df = analytics_df.withColumn(
                "cost_amount",
                (F.col("quantity") * F.col("unit_price") * F.lit(self.cost_ratio))
                .cast(DecimalType(16, 2))
            )
            
            analytics_df = analytics_df.withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    (((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
                ).otherwise(F.lit(0.0)).cast(DecimalType(5, 2))
            )
            
            # Categorize sales
            analytics_df = analytics_df.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold,
                    F.lit("HIGH")
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold,
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
            )
            
            # Add ETL run ID
            analytics_df = analytics_df.withColumn(
                "etl_run_id",
                F.lit(self.logger.get_etl_run_id())
            )
            
            # Select final columns
            analytics_df = analytics_df.select(
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
                "etl_run_id"
            )
            
            output_count = analytics_df.count()
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count,
                message=f'Transformed {output_count} of {input_count} records'
            )
            logging.info(f"Successfully transformed {output_count} records")
            
            return analytics_df
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=error_msg
            )
            logging.error(error_msg)
            raise TransformationError(error_msg) from e