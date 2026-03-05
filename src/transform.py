"""
Transform Module - Sales ETL System
Transforms raw sales data into analytics format
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, 
    expr, round as spark_round
)
from decimal import Decimal
import logging

from src.logger import ETLLogger
from src.schemas import AnalyticsSchema
from src.config import ETLConfig


class ETLTransformer:
    """Transforms raw sales data into analytics format"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: ETLConfig instance
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data to analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._calculate_analytics(raw_df)
            
            output_count = transformed_df.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.log.error(f"Transformation failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            return self.spark.createDataFrame([], AnalyticsSchema.get_schema()), False
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Calculate analytics metrics
        
        Args:
            df: Raw sales DataFrame
            
        Returns:
            DataFrame with analytics metrics
        """
        # Get business rules from config
        discount_tier1_qty = self.config.get("business_rules.discount_qty_tier1", 10)
        discount_tier2_qty = self.config.get("business_rules.discount_qty_tier2", 15)
        discount_tier1_rate = self.config.get("business_rules.discount_rate_tier1", 0.05)
        discount_tier2_rate = self.config.get("business_rules.discount_rate_tier2", 0.10)
        tax_rate = self.config.get("business_rules.tax_rate", 0.08)
        cost_ratio = self.config.get("business_rules.cost_ratio", 0.60)
        category_high = self.config.get("business_rules.category_high_threshold", 2000.00)
        category_medium = self.config.get("business_rules.category_medium_threshold", 500.00)
        
        # Calculate gross amount
        df = df.withColumn("gross_amount", col("quantity") * col("unit_price"))
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_tier2_qty, col("gross_amount") * discount_tier2_rate)
            .when(col("quantity") > discount_tier1_qty, col("gross_amount") * discount_tier1_rate)
            .otherwise(lit(0.0))
        )
        
        # Calculate tax on (gross - discount)
        df = df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * tax_rate
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            col("gross_amount") - col("discount_amount") + col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn("cost_amount", col("quantity") * col("unit_price") * cost_ratio)
        df = df.withColumn(
            "profit_margin",
            spark_round(
                when(col("net_amount") > 0, 
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100)
                .otherwise(lit(0.0)),
                2
            )
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= category_high, lit("HIGH"))
            .when(col("gross_amount") >= category_medium, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit(self.config.get("id_prefixes.analytics", "ANL")),
                col("trans_id"),
                expr("date_format(current_timestamp(), 'HHmmss')")
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.logger.etl_run_id))
        df = df.withColumn("loaded_at", current_timestamp())
        
        # Select and order columns according to schema
        df = df.select(
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
        
        return df
    
    def _categorize_sale(self, gross_amount: Decimal) -> str:
        """
        Categorize sale based on gross amount
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Category string (HIGH, MEDIUM, LOW)
        """
        category_high = self.config.get("business_rules.category_high_threshold", 2000.00)
        category_medium = self.config.get("business_rules.category_medium_threshold", 500.00)
        
        if gross_amount >= category_high:
            return "HIGH"
        elif gross_amount >= category_medium:
            return "MEDIUM"
        else:
            return "LOW"