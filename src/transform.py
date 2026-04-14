"""
Data transformation component.
Migrated from ABAP ZCL_ETL_TRANSFORMER class.
"""
from decimal import Decimal
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.constants import (
    ProcessStep,
    StatusCode,
    SaleCategory,
    BUSINESS_RULES
)
from src.logger import ETLLogger
from src.utils import generate_analytics_id


class ETLTransformer:
    """
    Transforms raw sales data into analytics format.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data to analytics format.
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            Tuple of (DataFrame with transformed data, success flag)
        """
        try:
            self.logger.log_message(
                step=ProcessStep.TRANSFORM.value,
                status=StatusCode.INFO.value,
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._apply_transformations(raw_df)
            
            output_count = transformed_df.count()
            
            self.logger.log_message(
                step=ProcessStep.TRANSFORM.value,
                status=StatusCode.SUCCESS.value,
                message=f"Transformed {output_count} of {input_count} records",
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.TRANSFORM.value,
                status=StatusCode.ERROR.value,
                message=f"Transformation failed: {str(e)}"
            )
            return raw_df.limit(0), False
    
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply business rule transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > BUSINESS_RULES.DISCOUNT_QTY_TIER2,
                F.col("gross_amount") * float(BUSINESS_RULES.DISCOUNT_RATE_TIER2)
            ).when(
                F.col("quantity") > BUSINESS_RULES.DISCOUNT_QTY_TIER1,
                F.col("gross_amount") * float(BUSINESS_RULES.DISCOUNT_RATE_TIER1)
            ).otherwise(0.0).cast(DecimalType(16, 2))
        )
        
        # Calculate tax amount
        df = df.withColumn(
            "tax_amount",
            ((F.col("gross_amount") - F.col("discount_amount")) * 
             float(BUSINESS_RULES.TAX_RATE)).cast(DecimalType(16, 2))
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount") + 
             F.col("tax_amount")).cast(DecimalType(16, 2))
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            (F.col("quantity") * F.col("unit_price") * 
             float(BUSINESS_RULES.COST_RATIO)).cast(DecimalType(16, 2))
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                (((F.col("net_amount") - F.col("cost_amount")) / 
                  F.col("net_amount")) * 100).cast(DecimalType(5, 2))
            ).otherwise(0.0)
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= float(BUSINESS_RULES.CATEGORY_HIGH_THRESHOLD),
                SaleCategory.HIGH.value
            ).when(
                F.col("gross_amount") >= float(BUSINESS_RULES.CATEGORY_MEDIUM_THRESHOLD),
                SaleCategory.MEDIUM.value
            ).otherwise(SaleCategory.LOW.value)
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", F.lit(self.logger.get_etl_run_id()))
        df = df.withColumn("loaded_at", F.current_timestamp())
        df = df.withColumn("loaded_by", F.lit("ETL_SYSTEM"))
        
        # Select final columns
        return df.select(
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
            "etl_run_id",
            "loaded_at",
            "loaded_by"
        )