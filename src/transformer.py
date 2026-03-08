"""
Transformer Module
Handles data transformation and business logic application.
"""
from datetime import datetime
from decimal import Decimal
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.constants import ETLStep, ETLStatus, BusinessRules, SaleCategory, IDPrefixes
from src.exceptions import TransformError


class ETLTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize transformer.
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed DataFrame, success_flag)
            
        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step=ETLStep.TRANSFORM,
                status=ETLStatus.SUCCESS,
                message="Starting data transformation"
            )
            
            total_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._apply_transformations(raw_df)
            
            # Validate transformed data
            success_count = transformed_df.count()
            error_count = total_count - success_count
            
            self.logger.log_message(
                step=ETLStep.TRANSFORM,
                status=ETLStatus.SUCCESS,
                message=f"Transformed {success_count} of {total_count} records",
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step=ETLStep.TRANSFORM,
                status=ETLStatus.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(f"Failed to transform data: {str(e)}", previous=e)
    
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply business logic transformations.
        
        Args:
            df: Raw sales DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > BusinessRules.DISCOUNT_QTY_TIER2,
                F.col("gross_amount") * float(BusinessRules.DISCOUNT_RATE_TIER2)
            ).when(
                F.col("quantity") > BusinessRules.DISCOUNT_QTY_TIER1,
                F.col("gross_amount") * float(BusinessRules.DISCOUNT_RATE_TIER1)
            ).otherwise(F.lit(0))
        )
        
        # Calculate tax
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * float(BusinessRules.TAX_RATE)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * float(BusinessRules.COST_RATIO)
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= float(BusinessRules.CATEGORY_HIGH_THRESHOLD),
                F.lit(SaleCategory.HIGH)
            ).when(
                F.col("gross_amount") >= float(BusinessRules.CATEGORY_MEDIUM_THRESHOLD),
                F.lit(SaleCategory.MEDIUM)
            ).otherwise(F.lit(SaleCategory.LOW))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit(IDPrefixes.ANALYTICS_ID),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", F.lit(self.logger.get_etl_run_id()))
        df = df.withColumn("loaded_at", F.current_timestamp())
        
        # Select and rename final columns
        analytics_df = df.select(
            "analytics_id",
            F.col("trans_date"),
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
            "etl_run_id",
            "loaded_at"
        )
        
        return analytics_df
    
    def get_analytics_schema(self) -> StructType:
        """Get schema for analytics data."""
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
            StructField("sales_rep", StringType(), False),
            StructField("region", StringType(), False),
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False)
        ])