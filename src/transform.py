"""
Sales ETL - Transform Module
Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, concat, current_timestamp,
    round as spark_round, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
from decimal import Decimal
import logging
from typing import Tuple

from src.utils.logger import ETLLogger
from src.utils.exceptions import TransformationError


class SalesTransformer:
    """Handles transformation of raw sales data to analytics format."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.etl_run_id = logger.etl_run_id
        
        # Business rule constants from config
        self.DISCOUNT_QTY_TIER1 = config.get("business_rules", {}).get("discount_qty_tier1", 10)
        self.DISCOUNT_QTY_TIER2 = config.get("business_rules", {}).get("discount_qty_tier2", 15)
        self.DISCOUNT_RATE_TIER1 = Decimal(str(config.get("business_rules", {}).get("discount_rate_tier1", 0.05)))
        self.DISCOUNT_RATE_TIER2 = Decimal(str(config.get("business_rules", {}).get("discount_rate_tier2", 0.10)))
        self.TAX_RATE = Decimal(str(config.get("business_rules", {}).get("tax_rate", 0.08)))
        self.COST_RATIO = Decimal(str(config.get("business_rules", {}).get("cost_ratio", 0.60)))
        self.CATEGORY_HIGH_THRESHOLD = Decimal(str(config.get("business_rules", {}).get("category_high_threshold", 2000.00)))
        self.CATEGORY_MEDIUM_THRESHOLD = Decimal(str(config.get("business_rules", {}).get("category_medium_threshold", 500.00)))
    
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), True)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, int, int]:
        """
        Transform raw sales data to analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed DataFrame, success count, error count)
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="I",
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(col("quantity") > self.DISCOUNT_QTY_TIER2,
                     spark_round(col("gross_amount") * lit(float(self.DISCOUNT_RATE_TIER2)), 2))
                .when(col("quantity") > self.DISCOUNT_QTY_TIER1,
                      spark_round(col("gross_amount") * lit(float(self.DISCOUNT_RATE_TIER1)), 2))
                .otherwise(lit(0.00))
            )
            
            # Calculate tax (on gross - discount)
            df = df.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(float(self.TAX_RATE)), 2)
            )
            
            # Calculate net amount (gross - discount + tax)
            df = df.withColumn(
                "net_amount",
                spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
            )
            
            # Calculate profit margin
            # Cost = quantity * unit_price * cost_ratio
            # Profit = net_amount - cost
            # Margin = (profit / net_amount) * 100
            df = df.withColumn(
                "cost",
                spark_round(col("quantity") * col("unit_price") * lit(float(self.COST_RATIO)), 2)
            )
            
            df = df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost")) / col("net_amount")) * 100, 2))
                .otherwise(lit(0.00))
            )
            
            # Categorize sales
            df = df.withColumn(
                "category",
                when(col("gross_amount") >= float(self.CATEGORY_HIGH_THRESHOLD), lit("HIGH"))
                .when(col("gross_amount") >= float(self.CATEGORY_MEDIUM_THRESHOLD), lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            df = df.withColumn(
                "analytics_id",
                concat(lit("ANL"), col("trans_id"))
            )
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", lit(self.etl_run_id)) \
                   .withColumn("loaded_at", current_timestamp()) \
                   .withColumn("loaded_by", lit("ETL_SYSTEM"))
            
            # Select final columns
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
            
            # Validate transformed data
            final_count = analytics_df.count()
            error_count = initial_count - final_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return analytics_df, final_count, error_count
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformationError(f"Failed to transform data: {str(e)}") from e
    
    def validate_transformed_data(self, df: DataFrame) -> bool:
        """
        Validate transformed data quality.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            True if validation passes
        """
        # Check for null values in critical columns
        null_checks = [
            "analytics_id", "customer_id", "product_id", 
            "gross_amount", "net_amount", "category"
        ]
        
        for col_name in null_checks:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                self.logger.log_message(
                    step="TRANSFORM",
                    status="W",
                    message=f"Found {null_count} null values in {col_name}"
                )
        
        # Check for negative amounts
        invalid_amounts = df.filter(
            (col("gross_amount") < 0) | (col("net_amount") < 0)
        ).count()
        
        if invalid_amounts > 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="W",
                message=f"Found {invalid_amounts} records with negative amounts"
            )
        
        # Check category values
        invalid_categories = df.filter(
            ~col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()
        
        if invalid_categories > 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="W",
                message=f"Found {invalid_categories} records with invalid categories"
            )
        
        return True