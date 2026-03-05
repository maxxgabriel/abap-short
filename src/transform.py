"""
Sales Data Transformation Module
Transforms raw sales data into analytics format with business logic
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, current_timestamp, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesDataTransformer:
    """Transforms raw sales data with business rules"""
    
    def __init__(self, logger: ETLLogger, config: ETLConfig):
        self.logger = logger
        self.config = config
        
    def get_analytics_schema(self) -> StructType:
        """Define schema for analytics data"""
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
            StructField("loaded_by", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame or None on failure
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations
            analytics_df = self._calculate_analytics(raw_df)
            
            # Validate results
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
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            logging.error(f"Transform error: {str(e)}", exc_info=True)
            return None
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """Apply business logic transformations"""
        
        # Get business rule configurations
        discount_tier1_qty = self.config.get("business_rules.discount_qty_tier1", 10)
        discount_tier2_qty = self.config.get("business_rules.discount_qty_tier2", 15)
        discount_tier1_rate = self.config.get("business_rules.discount_rate_tier1", 0.05)
        discount_tier2_rate = self.config.get("business_rules.discount_rate_tier2", 0.10)
        tax_rate = self.config.get("business_rules.tax_rate", 0.08)
        cost_ratio = self.config.get("business_rules.cost_ratio", 0.60)
        category_high = self.config.get("business_rules.category_high_threshold", 2000.00)
        category_medium = self.config.get("business_rules.category_medium_threshold", 500.00)
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_tier2_qty, 
                 spark_round(col("gross_amount") * lit(discount_tier2_rate), 2))
            .when(col("quantity") > discount_tier1_qty,
                  spark_round(col("gross_amount") * lit(discount_tier1_rate), 2))
            .otherwise(lit(0.00))
        )
        
        # Calculate tax (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * lit(tax_rate), 2)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            spark_round(
                col("gross_amount") - col("discount_amount") + col("tax_amount"), 2
            )
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * lit(cost_ratio), 2)
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2
                 ))
            .otherwise(lit(0.00))
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
            concat_ws("_", lit("ANL"), col("trans_id"))
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.logger.etl_run_id))
        df = df.withColumn("loaded_at", current_timestamp())
        df = df.withColumn("loaded_by", lit("PYSPARK_ETL"))
        
        # Select and rename columns to match target schema
        return df.select(
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
            col("loaded_at"),
            col("loaded_by")
        )