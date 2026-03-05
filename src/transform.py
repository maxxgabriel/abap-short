"""
ETL Transformer Module
Handles transformation of raw sales data into analytics format
"""

import uuid
from typing import Optional
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import TransformError


class SalesDataTransformer:
    """
    Transforms raw sales data into analytics format.
    
    Responsibilities:
    - Apply business rules for calculations
    - Calculate discounts, taxes, margins
    - Categorize sales
    - Generate analytics IDs
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the sales data transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: ETL configuration object
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    @staticmethod
    def get_analytics_schema() -> StructType:
        """
        Define schema for analytics data.
        
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
            StructField("etl_run_id", StringType(), nullable=False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
            
        Raises:
            TransformError: If transformation encounters critical error
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            # Apply transformations
            analytics_df = self._apply_transformations(raw_df)
            
            # Validate results
            if analytics_df is None:
                raise TransformError("Transformation produced null result")
            
            total_count = raw_df.count()
            success_count = analytics_df.count()
            error_count = total_count - success_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Transformed {success_count} of {total_count} records"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(f"Transformation error: {str(e)}")
    
    def _apply_transformations(self, raw_df: DataFrame) -> DataFrame:
        """
        Apply all business transformations to raw data.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame
        """
        business_rules = self.config.get_business_rules()
        
        # Calculate gross amount
        df = raw_df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > business_rules["discount_qty_tier2"],
                F.col("gross_amount") * business_rules["discount_rate_tier2"]
            ).when(
                F.col("quantity") > business_rules["discount_qty_tier1"],
                F.col("gross_amount") * business_rules["discount_rate_tier1"]
            ).otherwise(0.0)
        )
        
        # Calculate tax (applied after discount)
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * business_rules["tax_rate"]
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin (simplified: cost is 60% of unit price)
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * business_rules["cost_ratio"]
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(0.0)
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            self._categorize_sale_udf(F.col("gross_amount"), business_rules)
        )
        
        # Generate analytics ID using UUID
        df = df.withColumn(
            "analytics_id",
            F.concat(F.lit("ANL-"), F.col("trans_id"), F.lit("-"), F.expr("uuid()"))
        )
        
        # Add ETL run ID
        df = df.withColumn(
            "etl_run_id",
            F.lit(self.logger.etl_run_id)
        )
        
        # Select and rename columns to match analytics schema
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
        
        return analytics_df
    
    def _categorize_sale_udf(self, gross_amount_col, business_rules: dict):
        """
        Create UDF for categorizing sales based on gross amount.
        
        Args:
            gross_amount_col: Column containing gross amount
            business_rules: Business rules configuration
            
        Returns:
            Column expression for category
        """
        return F.when(
            gross_amount_col >= business_rules["category_high_threshold"],
            F.lit("HIGH")
        ).when(
            gross_amount_col >= business_rules["category_medium_threshold"],
            F.lit("MEDIUM")
        ).otherwise(F.lit("LOW"))