"""
Data transformation module for Sales ETL Pipeline.
Implements business logic from ABAP: discount tiers, tax calculation, profit margin.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, concat_ws, 
    current_timestamp, monotonically_increasing_id
)
from pyspark.sql.types import DecimalType
import logging

logger = logging.getLogger(__name__)


class SalesDataTransformer:
    """Transforms raw sales data into analytics format with business rules."""
    
    def __init__(self, config: dict):
        """
        Initialize the transformer with business rules.
        
        Args:
            config: Configuration dictionary with business rules
        """
        self.config = config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)
        
    def transform(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Apply business transformation logic to raw sales data.
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: Unique identifier for this ETL run
            
        Returns:
            Transformed analytics DataFrame
        """
        try:
            logger.info("Starting data transformation")
            
            # Calculate gross amount
            df_calc = df.withColumn(
                "gross_amount",
                spark_round((col("quantity") * col("unit_price")).cast(DecimalType(16, 2)), 2)
            )
            
            # Apply discount logic using when/otherwise
            df_calc = df_calc.withColumn(
                "discount_amount",
                when(col("quantity") > self.discount_qty_tier2, 
                     spark_round(col("gross_amount") * lit(self.discount_rate_tier2), 2))
                .when(col("quantity") > self.discount_qty_tier1,
                      spark_round(col("gross_amount") * lit(self.discount_rate_tier1), 2))
                .otherwise(lit(0.0).cast(DecimalType(16, 2)))
            )
            
            # Calculate tax amount (8% on gross - discount)
            df_calc = df_calc.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(self.tax_rate), 2)
            )
            
            # Calculate net amount
            df_calc = df_calc.withColumn(
                "net_amount",
                spark_round(
                    col("gross_amount") - col("discount_amount") + col("tax_amount"),
                    2
                )
            )
            
            # Calculate cost and profit margin
            df_calc = df_calc.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(self.cost_ratio), 2)
            )
            
            df_calc = df_calc.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(
                         ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100),
                         2
                     ))
                .otherwise(lit(0.0).cast(DecimalType(5, 2)))
            )
            
            # Categorize sales based on gross amount
            df_calc = df_calc.withColumn(
                "category",
                when(col("gross_amount") >= self.category_high_threshold, lit("HIGH"))
                .when(col("gross_amount") >= self.category_medium_threshold, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            df_calc = df_calc.withColumn(
                "analytics_id",
                concat_ws("_", lit("ANL"), col("trans_id"), monotonically_increasing_id())
            )
            
            # Add ETL metadata
            df_calc = df_calc.withColumn("etl_run_id", lit(etl_run_id))
            df_calc = df_calc.withColumn("loaded_at", current_timestamp())
            
            # Select and rename columns for analytics output
            df_analytics = df_calc.select(
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
            
            record_count = df_analytics.count()
            logger.info(f"Transformed {record_count} records successfully")
            
            return df_analytics
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise
    
    def get_transformation_summary(self, df: DataFrame) -> dict:
        """
        Generate summary statistics for transformed data.
        
        Args:
            df: Transformed analytics DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            from pyspark.sql.functions import count, sum as spark_sum, avg, min as spark_min, max as spark_max
            
            summary = df.agg(
                count("*").alias("total_records"),
                spark_sum("gross_amount").alias("total_gross"),
                spark_sum("discount_amount").alias("total_discount"),
                spark_sum("tax_amount").alias("total_tax"),
                spark_sum("net_amount").alias("total_net"),
                avg("profit_margin").alias("avg_profit_margin"),
                spark_min("net_amount").alias("min_sale"),
                spark_max("net_amount").alias("max_sale")
            ).collect()[0]
            
            category_dist = df.groupBy("category").count().collect()
            
            return {
                "total_records": summary.total_records,
                "total_gross": float(summary.total_gross or 0),
                "total_discount": float(summary.total_discount or 0),
                "total_tax": float(summary.total_tax or 0),
                "total_net": float(summary.total_net or 0),
                "avg_profit_margin": float(summary.avg_profit_margin or 0),
                "min_sale": float(summary.min_sale or 0),
                "max_sale": float(summary.max_sale or 0),
                "category_distribution": {row.category: row["count"] for row in category_dist}
            }
            
        except Exception as e:
            logger.error(f"Failed to generate summary: {str(e)}", exc_info=True)
            return {}