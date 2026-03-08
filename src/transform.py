"""
Data transformation module for Sales ETL process.
Transforms raw sales data into analytics format.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class DataTransformer:
    """Handles transformation of raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the data transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
        
        # Load business rules from config
        self.discount_tier1_qty = config.get("business_rules.discount_tier1_qty", 10)
        self.discount_tier2_qty = config.get("business_rules.discount_tier2_qty", 15)
        self.discount_rate_tier1 = config.get("business_rules.discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = config.get("business_rules.discount_rate_tier2", 0.10)
        self.tax_rate = config.get("business_rules.tax_rate", 0.08)
        self.cost_ratio = config.get("business_rules.cost_ratio", 0.60)
        self.category_high_threshold = config.get("business_rules.category_high_threshold", 2000.00)
        self.category_medium_threshold = config.get("business_rules.category_medium_threshold", 500.00)
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        
        Returns:
            StructType schema definition
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame, or None on failure
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Generate unique analytics ID
            df_with_id = raw_df.withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL"),
                    F.col("trans_id"),
                    F.date_format(F.current_timestamp(), "HHmmss")
                )
            )
            
            # Calculate gross amount
            df_with_gross = df_with_id.withColumn(
                "gross_amount",
                F.col("quantity") * F.col("unit_price")
            )
            
            # Calculate discount based on quantity tiers
            df_with_discount = df_with_gross.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_tier2_qty,
                    F.col("gross_amount") * self.discount_rate_tier2
                ).when(
                    F.col("quantity") > self.discount_tier1_qty,
                    F.col("gross_amount") * self.discount_rate_tier1
                ).otherwise(0)
            )
            
            # Calculate tax on discounted amount
            df_with_tax = df_with_discount.withColumn(
                "tax_amount",
                (F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate
            )
            
            # Calculate net amount
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
            )
            
            # Calculate profit margin
            df_with_profit = df_with_net.withColumn(
                "cost_amount",
                F.col("quantity") * F.col("unit_price") * self.cost_ratio
            ).withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
                ).otherwise(0)
            )
            
            # Categorize sales
            df_categorized = df_with_profit.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold,
                    F.lit("HIGH")
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold,
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
            )
            
            # Add ETL metadata
            analytics_df = df_categorized.withColumn(
                "etl_run_id",
                F.lit(self.logger.get_etl_run_id())
            ).withColumn(
                "loaded_at",
                F.current_timestamp()
            ).withColumn(
                "loaded_by",
                F.lit("PYSPARK_ETL")
            ).withColumn(
                "total_quantity",
                F.col("quantity")
            )
            
            # Select final columns
            final_df = analytics_df.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                "total_quantity",
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
                "loaded_at",
                "loaded_by"
            )
            
            output_count = final_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return final_df
            
        except Exception as e:
            self.log.error(f"Transformation failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            return None
    
    def validate_transformed_data(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed data and filter invalid records.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        valid_df = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return valid_df