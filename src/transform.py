"""
Data transformation module for ETL pipeline.
Transforms raw sales data into analytics format with business logic.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, round as spark_round, 
    concat, current_timestamp, expr, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Optional
import logging


class DataTransformer:
    """Handles transformation of raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger, etl_run_id: str):
        """
        Initialize the data transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.etl_run_id = etl_run_id
        self.schema = self._get_analytics_schema()
    
    def _get_analytics_schema(self) -> StructType:
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Get business rules from config
            discount_qty_tier1 = self.config.get('discount_qty_tier1', 10)
            discount_qty_tier2 = self.config.get('discount_qty_tier2', 15)
            discount_rate_tier1 = self.config.get('discount_rate_tier1', 0.05)
            discount_rate_tier2 = self.config.get('discount_rate_tier2', 0.10)
            tax_rate = self.config.get('tax_rate', 0.08)
            cost_ratio = self.config.get('cost_ratio', 0.60)
            category_high_threshold = self.config.get('category_high_threshold', 2000.00)
            category_medium_threshold = self.config.get('category_medium_threshold', 500.00)
            
            # Calculate gross amount
            df_with_gross = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            df_with_discount = df_with_gross.withColumn(
                "discount_amount",
                when(col("quantity") > discount_qty_tier2, 
                     spark_round(col("gross_amount") * discount_rate_tier2, 2))
                .when(col("quantity") > discount_qty_tier1,
                      spark_round(col("gross_amount") * discount_rate_tier1, 2))
                .otherwise(lit(0.0))
            )
            
            # Calculate tax amount
            df_with_tax = df_with_discount.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * tax_rate, 2)
            )
            
            # Calculate net amount
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                spark_round(
                    col("gross_amount") - col("discount_amount") + col("tax_amount"),
                    2
                )
            )
            
            # Calculate profit margin
            df_with_profit = df_with_net.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * cost_ratio, 2)
            ).withColumn(
                "profit_margin",
                spark_round(
                    when(col("net_amount") > 0,
                         ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100)
                    .otherwise(lit(0.0)),
                    2
                )
            )
            
            # Categorize sales
            df_with_category = df_with_profit.withColumn(
                "category",
                when(col("gross_amount") >= category_high_threshold, lit("HIGH"))
                .when(col("gross_amount") >= category_medium_threshold, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID and add metadata
            df_analytics = df_with_category.withColumn(
                "analytics_id",
                concat(lit("ANL"), col("trans_id"), 
                       expr("date_format(current_timestamp(), 'HHmmss')"))
            ).withColumn(
                "total_quantity", col("quantity")
            ).withColumn(
                "etl_run_id", lit(self.etl_run_id)
            ).withColumn(
                "loaded_at", current_timestamp()
            )
            
            # Select final columns
            result_df = df_analytics.select(
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
                "loaded_at"
            )
            
            record_count = result_df.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def validate_transformed_data(self, analytics_df: DataFrame) -> bool:
        """
        Validate transformed data for completeness and correctness.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check for null values in required fields
            null_counts = analytics_df.select([
                col(c).isNull().cast("int").alias(c) 
                for c in ["analytics_id", "customer_id", "product_id", "gross_amount"]
            ]).agg(*[expr(f"sum({c}) as {c}") for c in ["analytics_id", "customer_id", "product_id", "gross_amount"]])
            
            null_row = null_counts.collect()[0]
            has_nulls = any(null_row[c] > 0 for c in null_row.asDict().keys())
            
            if has_nulls:
                self.logger.warning("Validation found null values in required fields")
                return False
            
            # Check for invalid amounts
            invalid_amounts = analytics_df.filter(col("gross_amount") <= 0).count()
            if invalid_amounts > 0:
                self.logger.warning(f"Found {invalid_amounts} records with invalid gross amount")
                return False
            
            self.logger.info("Data validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False