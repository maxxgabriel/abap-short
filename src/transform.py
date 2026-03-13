"""
ETL Transformer Module
Transforms raw sales data into analytics format with business rules.
"""

import logging
from typing import Dict
from decimal import Decimal
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp,
    udf, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)

logger = logging.getLogger(__name__)


class SalesDataTransformer:
    """Transforms raw sales data applying business rules."""
    
    def __init__(self, config: Dict):
        """
        Initialize transformer.
        
        Args:
            config: Configuration dictionary with business rules
        """
        self.config = config
        self.schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
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
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
        ])
    
    def transform_data(
        self,
        raw_df: DataFrame,
        run_id: str
    ) -> DataFrame:
        """
        Transform raw data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame
        """
        try:
            logger.info(f"[{run_id}] Starting data transformation")
            
            # Get business rule parameters
            discount_tier1_qty = self.config.get('discount_qty_tier1', 10)
            discount_tier2_qty = self.config.get('discount_qty_tier2', 15)
            discount_rate_tier1 = Decimal(str(self.config.get('discount_rate_tier1', 0.05)))
            discount_rate_tier2 = Decimal(str(self.config.get('discount_rate_tier2', 0.10)))
            tax_rate = Decimal(str(self.config.get('tax_rate', 0.08)))
            cost_ratio = Decimal(str(self.config.get('cost_ratio', 0.60)))
            category_high = self.config.get('category_high_threshold', 2000.00)
            category_medium = self.config.get('category_medium_threshold', 500.00)
            
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(col("quantity") > discount_tier2_qty,
                     spark_round(col("gross_amount") * lit(float(discount_rate_tier2)), 2))
                .when(col("quantity") > discount_tier1_qty,
                      spark_round(col("gross_amount") * lit(float(discount_rate_tier1)), 2))
                .otherwise(lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            df = df.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(float(tax_rate)), 2)
            )
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
            )
            
            # Calculate cost and profit margin
            df = df.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(float(cost_ratio)), 2)
            )
            
            df = df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2))
                .otherwise(lit(0.0))
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
                concat(lit("ANL"), col("trans_id"))
            )
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", lit(run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            
            # Select and rename columns for output
            analytics_df = df.select(
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
            
            record_count = analytics_df.count()
            logger.info(f"[{run_id}] Transformed {record_count} records successfully")
            
            return analytics_df
            
        except Exception as e:
            logger.error(f"[{run_id}] Transformation failed: {str(e)}", exc_info=True)
            raise