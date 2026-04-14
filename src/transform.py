"""
Data transformation module for Sales ETL pipeline.
Applies business logic including discount, tax, and profit calculations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, concat, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
import logging
from typing import Dict, Tuple


class SalesTransformer:
    """Handles transformation of raw sales data into analytics format."""
    
    def __init__(self, logger: logging.Logger, config: Dict):
        """
        Initialize the transformer.
        
        Args:
            logger: Logger instance for tracking operations
            config: Configuration dictionary with business rules
        """
        self.logger = logger
        self.config = config
        self.schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
        """
        Define the schema for analytics data.
        
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
            StructField("profit_margin", DecimalType(5, 2), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=False)
        ])
    
    def transform_data(
        self,
        df_raw: DataFrame,
        etl_run_id: str
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: Unique identifier for this ETL run
        
        Returns:
            Tuple of (Transformed DataFrame, success flag)
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Extract configuration values
            discount_tier1_qty = self.config["business_rules"]["discount"]["tier1_quantity"]
            discount_tier2_qty = self.config["business_rules"]["discount"]["tier2_quantity"]
            discount_tier1_rate = self.config["business_rules"]["discount"]["tier1_rate"]
            discount_tier2_rate = self.config["business_rules"]["discount"]["tier2_rate"]
            tax_rate = self.config["business_rules"]["tax_rate"]
            cost_ratio = self.config["business_rules"]["cost_ratio"]
            category_high = self.config["business_rules"]["category"]["high_threshold"]
            category_medium = self.config["business_rules"]["category"]["medium_threshold"]
            
            # Calculate gross amount
            df_transformed = df_raw.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            df_transformed = df_transformed.withColumn(
                "discount_amount",
                when(col("quantity") > discount_tier2_qty,
                     spark_round(col("gross_amount") * lit(discount_tier2_rate), 2))
                .when(col("quantity") > discount_tier1_qty,
                      spark_round(col("gross_amount") * lit(discount_tier1_rate), 2))
                .otherwise(lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            df_transformed = df_transformed.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(tax_rate), 2)
            )
            
            # Calculate net amount (gross - discount + tax)
            df_transformed = df_transformed.withColumn(
                "net_amount",
                spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
            )
            
            # Calculate cost and profit margin
            df_transformed = df_transformed.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(cost_ratio), 2)
            )
            
            df_transformed = df_transformed.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2))
                .otherwise(lit(0.0))
            )
            
            # Categorize sales based on gross amount
            df_transformed = df_transformed.withColumn(
                "category",
                when(col("gross_amount") >= category_high, lit("HIGH"))
                .when(col("gross_amount") >= category_medium, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            df_transformed = df_transformed.withColumn(
                "analytics_id",
                concat(lit("ANL"), col("trans_id"))
            )
            
            # Add ETL metadata
            df_transformed = df_transformed.withColumn(
                "etl_run_id",
                lit(etl_run_id)
            ).withColumn(
                "loaded_at",
                current_timestamp()
            )
            
            # Select final columns
            df_analytics = df_transformed.select(
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
                "loaded_at"
            )
            
            record_count = df_analytics.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return df_analytics, True
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            return df_raw.sparkSession.createDataFrame([], self.schema), False
    
    def validate_transformed_data(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Validate transformed data and filter out invalid records.
        
        Args:
            df: Transformed DataFrame
        
        Returns:
            Tuple of (Valid DataFrame, count of invalid records)
        """
        try:
            self.logger.info("Validating transformed data")
            
            initial_count = df.count()
            
            # Apply validation rules
            df_valid = df.filter(
                (col("analytics_id").isNotNull()) &
                (col("customer_id").isNotNull()) &
                (col("product_id").isNotNull()) &
                (col("gross_amount") > 0) &
                (col("currency").isNotNull()) &
                (col("category").isin("HIGH", "MEDIUM", "LOW"))
            )
            
            valid_count = df_valid.count()
            invalid_count = initial_count - valid_count
            
            if invalid_count > 0:
                self.logger.warning(f"Found {invalid_count} invalid records")
            
            self.logger.info(f"Validation complete: {valid_count} valid records")
            
            return df_valid, invalid_count
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return df, 0