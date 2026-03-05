"""
Data transformation module for Sales ETL pipeline.
Applies business logic to convert raw sales data into analytics format.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, expr, concat, current_timestamp,
    date_format, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
from typing import Dict
import logging


class SalesDataTransformer:
    """Transforms raw sales data using business rules."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = config.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = config.get("discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = config.get("discount_rate_tier2", 0.10)
        self.tax_rate = config.get("tax_rate", 0.08)
        self.cost_ratio = config.get("cost_ratio", 0.60)
        self.category_high_threshold = config.get(
            "category_high_threshold", 2000.00
        )
        self.category_medium_threshold = config.get(
            "category_medium_threshold", 500.00
        )
    
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
    
    def transform_data(
        self, 
        raw_df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Transform raw sales data to analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(
                    col("quantity") > self.discount_qty_tier2,
                    spark_round(col("gross_amount") * self.discount_rate_tier2, 2)
                ).when(
                    col("quantity") > self.discount_qty_tier1,
                    spark_round(col("gross_amount") * self.discount_rate_tier1, 2)
                ).otherwise(lit(0.00))
            )
            
            # Calculate tax on (gross - discount)
            df = df.withColumn(
                "tax_amount",
                spark_round(
                    (col("gross_amount") - col("discount_amount")) * self.tax_rate,
                    2
                )
            )
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                spark_round(
                    col("gross_amount") - col("discount_amount") + col("tax_amount"),
                    2
                )
            )
            
            # Calculate profit margin
            # Profit Margin = ((Net - Cost) / Net) * 100
            # Cost = quantity * unit_price * cost_ratio
            df = df.withColumn(
                "cost_amount",
                spark_round(
                    col("quantity") * col("unit_price") * self.cost_ratio,
                    2
                )
            )
            
            df = df.withColumn(
                "profit_margin",
                when(
                    col("net_amount") > 0,
                    spark_round(
                        ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                        2
                    )
                ).otherwise(lit(0.00))
            )
            
            # Categorize sales based on gross amount
            df = df.withColumn(
                "category",
                when(
                    col("gross_amount") >= self.category_high_threshold,
                    lit("HIGH")
                ).when(
                    col("gross_amount") >= self.category_medium_threshold,
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
            df = df.withColumn("etl_run_id", lit(etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            df = df.withColumn("loaded_by", lit("PYSPARK_ETL"))
            
            # Select and rename columns to match analytics schema
            analytics_df = df.select(
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
                "etl_run_id",
                "loaded_at",
                "loaded_by"
            )
            
            record_count = analytics_df.count()
            self.logger.info(
                f"Transformed {record_count} records successfully"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def validate_transformed_data(self, df: DataFrame) -> Dict[str, int]:
        """
        Validate transformed data quality and calculate statistics.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Dictionary with validation statistics
        """
        stats = {}
        
        # Count records by category
        category_counts = df.groupBy("category").count().collect()
        for row in category_counts:
            stats[f"category_{row['category'].lower()}"] = row["count"]
        
        # Check for negative amounts
        negative_count = df.filter(
            (col("gross_amount") < 0) |
            (col("net_amount") < 0) |
            (col("discount_amount") < 0) |
            (col("tax_amount") < 0)
        ).count()
        stats["negative_amounts"] = negative_count
        
        # Check profit margin range
        invalid_margin = df.filter(
            (col("profit_margin") < -100) | (col("profit_margin") > 100)
        ).count()
        stats["invalid_profit_margin"] = invalid_margin
        
        # Calculate average metrics
        agg_stats = df.agg({
            "gross_amount": "avg",
            "net_amount": "avg",
            "discount_amount": "avg",
            "profit_margin": "avg"
        }).first()
        
        stats["avg_gross_amount"] = float(agg_stats["avg(gross_amount)"])
        stats["avg_net_amount"] = float(agg_stats["avg(net_amount)"])
        stats["avg_discount"] = float(agg_stats["avg(discount_amount)"])
        stats["avg_profit_margin"] = float(agg_stats["avg(profit_margin)"])
        
        self.logger.info(f"Validation statistics: {stats}")
        
        return stats