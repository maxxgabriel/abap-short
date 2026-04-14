"""
PySpark Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import logging


class SalesDataTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.analytics_schema = self._define_analytics_schema()
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = config.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = config.get("discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = config.get("discount_rate_tier2", 0.10)
        self.tax_rate = config.get("tax_rate", 0.08)
        self.cost_ratio = config.get("cost_ratio", 0.60)
        self.category_high_threshold = config.get("category_high_threshold", 2000.00)
        self.category_medium_threshold = config.get("category_medium_threshold", 500.00)
    
    def _define_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data table.
        Corresponds to ZSALES_ANALYTICS table structure.
        
        Returns:
            StructType schema definition
        """
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
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", StringType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    def transform_data(self, raw_df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        Applies all business rules and calculations.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                F.col("quantity") * F.col("unit_price")
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_qty_tier2,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier2)
                ).when(
                    F.col("quantity") > self.discount_qty_tier1,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier1)
                ).otherwise(F.lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            df = df.withColumn(
                "tax_amount",
                (F.col("gross_amount") - F.col("discount_amount")) * F.lit(self.tax_rate)
            )
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
            )
            
            # Calculate cost and profit margin
            df = df.withColumn(
                "cost_amount",
                F.col("quantity") * F.col("unit_price") * F.lit(self.cost_ratio)
            )
            
            df = df.withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
                ).otherwise(F.lit(0.0))
            )
            
            # Categorize sales
            df = df.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold,
                    F.lit("HIGH")
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold,
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
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
            df = df.withColumn("loaded_at", F.current_timestamp().cast("string"))
            df = df.withColumn("loaded_by", F.lit("pyspark_etl"))
            
            # Rename quantity column
            df = df.withColumnRenamed("quantity", "total_quantity")
            
            # Select final columns in correct order
            analytics_df = df.select(
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
            
            record_count = analytics_df.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return analytics_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def aggregate_by_customer(self, analytics_df: DataFrame) -> DataFrame:
        """
        Aggregate analytics data by customer.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Customer-aggregated DataFrame
        """
        customer_agg = analytics_df.groupBy("customer_id").agg(
            F.count("analytics_id").alias("transaction_count"),
            F.sum("total_quantity").alias("total_quantity"),
            F.sum("gross_amount").alias("total_gross_amount"),
            F.sum("net_amount").alias("total_net_amount"),
            F.sum("discount_amount").alias("total_discount"),
            F.avg("profit_margin").alias("avg_profit_margin"),
            F.collect_set("region").alias("regions"),
            F.min("trans_date").alias("first_transaction_date"),
            F.max("trans_date").alias("last_transaction_date")
        )
        
        return customer_agg
    
    def aggregate_by_product(self, analytics_df: DataFrame) -> DataFrame:
        """
        Aggregate analytics data by product.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Product-aggregated DataFrame
        """
        product_agg = analytics_df.groupBy("product_id").agg(
            F.count("analytics_id").alias("transaction_count"),
            F.sum("total_quantity").alias("total_quantity_sold"),
            F.sum("gross_amount").alias("total_gross_amount"),
            F.sum("net_amount").alias("total_net_amount"),
            F.avg("profit_margin").alias("avg_profit_margin"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.collect_set("region").alias("regions_sold")
        )
        
        return product_agg
    
    def aggregate_by_region(self, analytics_df: DataFrame) -> DataFrame:
        """
        Aggregate analytics data by region.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Region-aggregated DataFrame
        """
        region_agg = analytics_df.groupBy("region").agg(
            F.count("analytics_id").alias("transaction_count"),
            F.sum("total_quantity").alias("total_quantity"),
            F.sum("gross_amount").alias("total_gross_amount"),
            F.sum("net_amount").alias("total_net_amount"),
            F.sum("discount_amount").alias("total_discount"),
            F.avg("profit_margin").alias("avg_profit_margin"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("sales_rep").alias("sales_rep_count")
        )
        
        return region_agg
    
    def calculate_running_totals(self, analytics_df: DataFrame) -> DataFrame:
        """
        Calculate running totals by customer over time.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            DataFrame with running totals
        """
        window_spec = Window.partitionBy("customer_id").orderBy("trans_date")
        
        df_with_running = analytics_df.withColumn(
            "running_total_amount",
            F.sum("net_amount").over(window_spec)
        ).withColumn(
            "running_transaction_count",
            F.count("analytics_id").over(window_spec)
        )
        
        return df_with_running
    
    def validate_transformed_data(self, analytics_df: DataFrame) -> dict:
        """
        Validate transformed analytics data.
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "total_records": analytics_df.count(),
            "null_analytics_ids": analytics_df.filter(
                F.col("analytics_id").isNull()
            ).count(),
            "invalid_gross_amounts": analytics_df.filter(
                F.col("gross_amount") <= 0
            ).count(),
            "invalid_net_amounts": analytics_df.filter(
                F.col("net_amount") <= 0
            ).count(),
            "invalid_categories": analytics_df.filter(
                ~F.col("category").isin(["HIGH", "MEDIUM", "LOW"])
            ).count(),
            "negative_profit_margins": analytics_df.filter(
                F.col("profit_margin") < 0
            ).count()
        }
        
        self.logger.info(f"Transformation validation results: {validation_results}")
        
        return validation_results