"""
Data Transformation Module
Transforms raw sales data into analytics format
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from typing import Dict
import logging
from datetime import datetime


class SalesDataTransformer:
    """
    Transforms raw sales data into analytics format.
    Equivalent to ZCL_ETL_TRANSFORMER in ABAP.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Business rules from ZCL_ETL_CONSTANTS
        self.discount_qty_tier1 = config.get('business_rules', {}).get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('business_rules', {}).get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('business_rules', {}).get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('business_rules', {}).get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('business_rules', {}).get('tax_rate', 0.08)
        self.cost_ratio = config.get('business_rules', {}).get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('business_rules', {}).get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('business_rules', {}).get('category_medium_threshold', 500.00)
    
    def transform_data(
        self,
        raw_df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: Current ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_qty_tier2,
                    F.col("gross_amount") * self.discount_rate_tier2
                ).when(
                    F.col("quantity") > self.discount_qty_tier1,
                    F.col("gross_amount") * self.discount_rate_tier1
                ).otherwise(0.0).cast(DecimalType(16, 2))
            )
            
            # Calculate tax (on gross minus discount)
            df = df.withColumn(
                "tax_amount",
                ((F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate).cast(DecimalType(16, 2))
            )
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")).cast(DecimalType(16, 2))
            )
            
            # Calculate cost and profit margin
            df = df.withColumn(
                "cost_amount",
                (F.col("quantity") * F.col("unit_price") * self.cost_ratio).cast(DecimalType(16, 2))
            )
            
            df = df.withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    (((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
                ).otherwise(0.0).cast(DecimalType(5, 2))
            )
            
            # Categorize sales
            df = df.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold, "HIGH"
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold, "MEDIUM"
                ).otherwise("LOW")
            )
            
            # Generate analytics ID
            df = df.withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL"),
                    F.col("trans_id"),
                    F.date_format(F.current_timestamp(), "HHmmss")
                )
            )
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", F.lit(etl_run_id))
            
            # Select final columns matching ZSALES_ANALYTICS structure
            analytics_df = df.select(
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
            
            record_count = analytics_df.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return analytics_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise