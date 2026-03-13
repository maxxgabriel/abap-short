"""
Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from typing import Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger


class Transformer:
    """
    Handles data transformation with business rule application.
    """
    
    def __init__(self, spark, logger: ETLLogger, config: dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.business_rules = config.get("business_rules", {})
    
    @staticmethod
    def get_analytics_schema() -> StructType:
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
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(
        self,
        raw_data: DataFrame
    ) -> Tuple[bool, Optional[DataFrame]]:
        """
        Transform raw data into analytics format.
        
        Args:
            raw_data: Raw sales DataFrame
        
        Returns:
            Tuple of (success flag, transformed DataFrame or None)
        """
        try:
            self.logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_SUCCESS,
                message="Starting data transformation"
            )
            
            raw_count = raw_data.count()
            
            # Apply transformations
            analytics_data = self._apply_transformations(raw_data)
            
            # Validate transformed data
            analytics_data = analytics_data.filter(
                F.col("gross_amount").isNotNull() &
                F.col("net_amount").isNotNull()
            )
            
            transformed_count = analytics_data.count()
            error_count = raw_count - transformed_count
            
            self.logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_SUCCESS,
                records_processed=raw_count,
                records_success=transformed_count,
                records_error=error_count,
                message=f"Transformed {transformed_count} of {raw_count} records"
            )
            
            return True, analytics_data
            
        except Exception as e:
            self.logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return False, None
    
    def _apply_transformations(self, raw_data: DataFrame) -> DataFrame:
        """
        Apply business rules and transformations.
        
        Args:
            raw_data: Raw DataFrame
        
        Returns:
            Transformed DataFrame
        """
        # Get business rules
        discount_rules = self.business_rules.get("discount", {})
        tax_rate = self.business_rules.get("tax", {}).get("rate", 0.08)
        cost_ratio = self.business_rules.get("cost", {}).get("ratio", 0.60)
        category_rules = self.business_rules.get("category", {})
        
        qty_tier1 = discount_rules.get("quantity_tier1", 10)
        qty_tier2 = discount_rules.get("quantity_tier2", 15)
        rate_tier1 = discount_rules.get("rate_tier1", 0.05)
        rate_tier2 = discount_rules.get("rate_tier2", 0.10)
        
        high_threshold = category_rules.get("high_threshold", 2000.00)
        medium_threshold = category_rules.get("medium_threshold", 500.00)
        
        # Calculate analytics fields
        transformed = raw_data \
            .withColumn("gross_amount", 
                       F.col("quantity") * F.col("unit_price")) \
            .withColumn("discount_amount",
                       F.when(F.col("quantity") > qty_tier2, 
                             F.col("gross_amount") * F.lit(rate_tier2))
                        .when(F.col("quantity") > qty_tier1,
                             F.col("gross_amount") * F.lit(rate_tier1))
                        .otherwise(F.lit(0))) \
            .withColumn("tax_amount",
                       (F.col("gross_amount") - F.col("discount_amount")) * F.lit(tax_rate)) \
            .withColumn("net_amount",
                       F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")) \
            .withColumn("cost_amount",
                       F.col("quantity") * F.col("unit_price") * F.lit(cost_ratio)) \
            .withColumn("profit_margin",
                       F.when(F.col("net_amount") > 0,
                             ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount") * 100))
                        .otherwise(F.lit(0))) \
            .withColumn("category",
                       F.when(F.col("gross_amount") >= high_threshold, F.lit("HIGH"))
                        .when(F.col("gross_amount") >= medium_threshold, F.lit("MEDIUM"))
                        .otherwise(F.lit("LOW"))) \
            .withColumn("analytics_id",
                       F.concat(F.lit("ANL"), F.col("trans_id"), 
                               F.date_format(F.current_timestamp(), "HHmmss"))) \
            .withColumn("etl_run_id", F.lit(self.logger.get_etl_run_id()))
        
        # Select final columns
        return transformed.select(
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