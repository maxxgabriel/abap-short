"""
Data transformation module for ETL system.

This module handles transformation of raw sales data into analytics format.
Replaces ABAP class: ZCL_ETL_TRANSFORMER
"""

from typing import Tuple
from decimal import Decimal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, udf
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType

from src.exceptions import TransformError
from src.logger import ETLLogger


class SalesTransformer:
    """
    Transforms raw sales data into analytics format.
    
    This class handles the TRANSFORM phase of the ETL process, applying
    business rules, calculations, and data enrichment.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger for tracking operations
        config: Configuration parameters for transformations
    """
    
    # Schema for analytics data
    ANALYTICS_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer with Spark session, logger, and config.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def transform_data(
        self,
        raw_df: DataFrame,
        etl_run_id: str
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Applies business rules:
        - Discount calculation based on quantity tiers
        - Tax calculation
        - Net amount calculation
        - Profit margin calculation
        - Sale categorization
        
        Args:
            raw_df: DataFrame with raw sales data
            etl_run_id: Unique identifier for this ETL run
            
        Returns:
            Tuple of (DataFrame with analytics data, success boolean)
            
        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations using DataFrame API
            transformed_df = (
                raw_df
                .withColumn("gross_amount", col("quantity") * col("unit_price"))
                .withColumn("discount_amount", self._calculate_discount())
                .withColumn("tax_amount", self._calculate_tax())
                .withColumn("net_amount", self._calculate_net_amount())
                .withColumn("profit_margin", self._calculate_profit_margin())
                .withColumn("category", self._categorize_sale())
                .withColumn("analytics_id", self._generate_analytics_id())
                .withColumn("etl_run_id", lit(etl_run_id))
                .withColumn("loaded_at", current_timestamp())
                .select(
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
            )
            
            # Cache for performance
            transformed_df.cache()
            
            final_count = transformed_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                records_error=initial_count - final_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(
                message=f"Data transformation failed: {str(e)}",
                context={"etl_run_id": etl_run_id}
            )
    
    def _calculate_discount(self):
        """
        Calculate discount amount based on quantity tiers.
        
        Business rules:
        - Quantity > 15: 10% discount
        - Quantity > 10: 5% discount
        - Otherwise: No discount
        
        Returns:
            Column expression for discount amount
        """
        discount_qty_tier2 = self.config.get("discount_qty_tier2", 15)
        discount_qty_tier1 = self.config.get("discount_qty_tier1", 10)
        discount_rate_tier2 = Decimal(str(self.config.get("discount_rate_tier2", 0.10)))
        discount_rate_tier1 = Decimal(str(self.config.get("discount_rate_tier1", 0.05)))
        
        return when(
            col("quantity") > discount_qty_tier2,
            col("gross_amount") * lit(discount_rate_tier2)
        ).when(
            col("quantity") > discount_qty_tier1,
            col("gross_amount") * lit(discount_rate_tier1)
        ).otherwise(lit(0))
    
    def _calculate_tax(self):
        """
        Calculate tax amount on gross minus discount.
        
        Business rule: 8% tax on (gross - discount)
        
        Returns:
            Column expression for tax amount
        """
        tax_rate = Decimal(str(self.config.get("tax_rate", 0.08)))
        
        return (col("gross_amount") - col("discount_amount")) * lit(tax_rate)
    
    def _calculate_net_amount(self):
        """
        Calculate net amount: gross - discount + tax.
        
        Returns:
            Column expression for net amount
        """
        return col("gross_amount") - col("discount_amount") + col("tax_amount")
    
    def _calculate_profit_margin(self):
        """
        Calculate profit margin percentage.
        
        Business rule: Assume cost is 60% of unit price
        Profit margin = ((net - cost) / net) * 100
        
        Returns:
            Column expression for profit margin
        """
        cost_ratio = Decimal(str(self.config.get("cost_ratio", 0.60)))
        cost = col("quantity") * col("unit_price") * lit(cost_ratio)
        
        return when(
            col("net_amount") > 0,
            ((col("net_amount") - cost) / col("net_amount")) * lit(100)
        ).otherwise(lit(0))
    
    def _categorize_sale(self):
        """
        Categorize sale based on gross amount.
        
        Business rules:
        - >= 2000: HIGH
        - >= 500: MEDIUM
        - < 500: LOW
        
        Returns:
            Column expression for category
        """
        high_threshold = self.config.get("category_high_threshold", 2000.00)
        medium_threshold = self.config.get("category_medium_threshold", 500.00)
        
        return when(
            col("gross_amount") >= high_threshold,
            lit("HIGH")
        ).when(
            col("gross_amount") >= medium_threshold,
            lit("MEDIUM")
        ).otherwise(lit("LOW"))
    
    def _generate_analytics_id(self):
        """
        Generate unique analytics ID.
        
        Format: ANL{trans_id}{timestamp_suffix}
        
        Returns:
            Column expression for analytics ID
        """
        return concat(
            lit("ANL"),
            col("trans_id"),
            lit("_"),
            current_timestamp().cast("string")
        )