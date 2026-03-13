"""
Transform module for Sales ETL process.
Transforms raw sales data into analytics format with business logic.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp,
    year, month, dayofmonth, hour, minute, second
)
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
import logging


class SalesTransformer:
    """Handles transformation of raw sales data to analytics format."""
    
    def __init__(self, config: dict, etl_run_id: str):
        """
        Initialize transformer.
        
        Args:
            config: Configuration dictionary
            etl_run_id: Unique ETL run identifier
        """
        self.config = config
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(__name__)
        
    def get_schema(self) -> StructType:
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data to analytics format.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Business rule thresholds from config
            discount_tier1_qty = self.config['business_rules']['discount_tier1_qty']
            discount_tier2_qty = self.config['business_rules']['discount_tier2_qty']
            discount_tier1_rate = self.config['business_rules']['discount_tier1_rate']
            discount_tier2_rate = self.config['business_rules']['discount_tier2_rate']
            tax_rate = self.config['business_rules']['tax_rate']
            cost_ratio = self.config['business_rules']['cost_ratio']
            category_high_threshold = self.config['business_rules']['category_high_threshold']
            category_medium_threshold = self.config['business_rules']['category_medium_threshold']
            
            # Calculate gross amount
            transformed_df = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            
            # Calculate discount based on quantity tiers
            transformed_df = transformed_df.withColumn(
                "discount_amount",
                when(col("quantity") > discount_tier2_qty, 
                     col("gross_amount") * lit(discount_tier2_rate))
                .when(col("quantity") > discount_tier1_qty,
                      col("gross_amount") * lit(discount_tier1_rate))
                .otherwise(lit(0))
            )
            
            # Calculate tax on (gross - discount)
            transformed_df = transformed_df.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * lit(tax_rate)
            )
            
            # Calculate net amount
            transformed_df = transformed_df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Calculate profit margin (simplified: cost is 60% of unit price)
            transformed_df = transformed_df.withColumn(
                "cost_amount",
                col("quantity") * col("unit_price") * lit(cost_ratio)
            )
            
            transformed_df = transformed_df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100))
                .otherwise(lit(0))
            )
            
            # Categorize sales
            transformed_df = transformed_df.withColumn(
                "category",
                when(col("gross_amount") >= category_high_threshold, lit("HIGH"))
                .when(col("gross_amount") >= category_medium_threshold, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics_id (ANL + trans_id + timestamp suffix)
            transformed_df = transformed_df.withColumn(
                "timestamp_suffix",
                concat(
                    year(current_timestamp()).cast("string"),
                    month(current_timestamp()).cast("string"),
                    dayofmonth(current_timestamp()).cast("string"),
                    hour(current_timestamp()).cast("string"),
                    minute(current_timestamp()).cast("string"),
                    second(current_timestamp()).cast("string")
                )
            )
            
            transformed_df = transformed_df.withColumn(
                "analytics_id",
                concat(lit("ANL"), col("trans_id"), col("timestamp_suffix"))
            )
            
            # Add ETL run ID
            transformed_df = transformed_df.withColumn(
                "etl_run_id",
                lit(self.etl_run_id)
            )
            
            # Select final columns
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
            
            record_count = analytics_df.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return analytics_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise