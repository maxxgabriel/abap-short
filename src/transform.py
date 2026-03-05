"""
Sales Data Transformer Module
Transforms raw sales data into analytics format with business logic.
"""

from datetime import datetime
from typing import Optional
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import TransformationError


class SalesTransformer:
    """
    Transforms raw sales data into analytics format.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger instance
        config: Configuration dictionary
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the sales transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Business rules from config
        self.discount_qty_tier1 = config.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = config.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = Decimal(str(config.get("discount_rate_tier1", "0.05")))
        self.discount_rate_tier2 = Decimal(str(config.get("discount_rate_tier2", "0.10")))
        self.tax_rate = Decimal(str(config.get("tax_rate", "0.08")))
        self.cost_ratio = Decimal(str(config.get("cost_ratio", "0.60")))
        self.category_high_threshold = Decimal(str(config.get("category_high_threshold", "2000.00")))
        self.category_medium_threshold = Decimal(str(config.get("category_medium_threshold", "500.00")))
    
    def transform_data(self, raw_data: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_data: Raw sales DataFrame
        
        Returns:
            Transformed analytics DataFrame, or None if transformation fails
        
        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            total_count = raw_data.count()
            
            # Apply transformations
            analytics_data = self._apply_transformations(raw_data)
            
            success_count = analytics_data.count()
            error_count = total_count - success_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Transformed {success_count} of {total_count} records"
            )
            
            return analytics_data
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=error_msg
            )
            raise TransformationError(
                error_text=error_msg,
                error_step="TRANSFORM"
            ) from e
    
    def _apply_transformations(self, raw_data: DataFrame) -> DataFrame:
        """
        Apply business transformation logic to raw data.
        
        Args:
            raw_data: Raw sales DataFrame
        
        Returns:
            Transformed DataFrame with analytics fields
        """
        # Calculate gross amount
        df = raw_data.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.discount_qty_tier2,
                F.col("gross_amount") * F.lit(float(self.discount_rate_tier2))
            ).when(
                F.col("quantity") > self.discount_qty_tier1,
                F.col("gross_amount") * F.lit(float(self.discount_rate_tier1))
            ).otherwise(F.lit(0.0))
        )
        
        # Calculate tax amount
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * F.lit(float(self.tax_rate))
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(float(self.cost_ratio))
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
                F.col("gross_amount") >= float(self.category_high_threshold),
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= float(self.category_medium_threshold),
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL_"),
                F.col("trans_id"),
                F.lit("_"),
                F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
            )
        )
        
        # Add ETL run ID
        df = df.withColumn(
            "etl_run_id",
            F.lit(self.logger.get_etl_run_id())
        )
        
        # Select final columns
        analytics_data = df.select(
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
        
        return analytics_data