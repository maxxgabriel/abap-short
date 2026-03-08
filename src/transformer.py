from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Tuple
from src.logger import ETLLogger
from src.constants import (
    ProcessStep, ProcessStatus, SaleCategory, ETLConstants
)
from src.exceptions import TransformError
from src.models import SchemaDefinitions


class DataTransformer:
    """Transforms raw sales data into analytics format"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        self.spark = spark
        self.logger = logger
        self.schema = SchemaDefinitions.analytics_schema()
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data to analytics format
        
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step=ProcessStep.TRANSFORM,
                status=ProcessStatus.SUCCESS,
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations
            analytics_df = (
                raw_df
                .withColumn("gross_amount", 
                    F.col("quantity") * F.col("unit_price"))
                .withColumn("discount_amount",
                    self._calculate_discount())
                .withColumn("tax_amount",
                    (F.col("gross_amount") - F.col("discount_amount")) 
                    * F.lit(ETLConstants.TAX_RATE))
                .withColumn("net_amount",
                    F.col("gross_amount") - F.col("discount_amount") 
                    + F.col("tax_amount"))
                .withColumn("cost_amount",
                    F.col("quantity") * F.col("unit_price") 
                    * F.lit(ETLConstants.COST_RATIO))
                .withColumn("profit_margin",
                    ((F.col("net_amount") - F.col("cost_amount")) 
                     / F.col("net_amount")) * 100)
                .withColumn("category",
                    self._categorize_sale())
                .withColumn("analytics_id",
                    F.concat(
                        F.lit(ETLConstants.PREFIX_ANALYTICS_ID),
                        F.col("trans_id"),
                        F.date_format(F.current_timestamp(), "HHmmss")
                    ))
                .withColumn("etl_run_id", 
                    F.lit(self.logger.get_etl_run_id()))
                .withColumn("loaded_at", F.current_timestamp())
                .withColumn("loaded_by", F.lit("system"))
                .select(
                    "analytics_id", "trans_date", "customer_id", 
                    "product_id", F.col("quantity").alias("total_quantity"),
                    "gross_amount", "net_amount", "discount_amount",
                    "tax_amount", "currency", "sales_rep", "region",
                    "profit_margin", "category", "etl_run_id",
                    "loaded_at", "loaded_by"
                )
            )
            
            final_count = analytics_df.count()
            
            self.logger.log_message(
                step=ProcessStep.TRANSFORM,
                status=ProcessStatus.SUCCESS,
                message=f"Transformed {final_count} of {initial_count} records",
                records_processed=initial_count,
                records_success=final_count,
                records_error=initial_count - final_count
            )
            
            return analytics_df, True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.TRANSFORM,
                status=ProcessStatus.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(
                message=f"Failed to transform data: {str(e)}",
                step=ProcessStep.TRANSFORM
            )
    
    def _calculate_discount(self):
        """Calculate discount based on quantity tiers"""
        return F.when(
            F.col("quantity") > ETLConstants.DISCOUNT_QTY_TIER2,
            F.col("quantity") * F.col("unit_price") 
            * F.lit(ETLConstants.DISCOUNT_RATE_TIER2)
        ).when(
            F.col("quantity") > ETLConstants.DISCOUNT_QTY_TIER1,
            F.col("quantity") * F.col("unit_price") 
            * F.lit(ETLConstants.DISCOUNT_RATE_TIER1)
        ).otherwise(F.lit(0))
    
    def _categorize_sale(self):
        """Categorize sale based on gross amount"""
        return F.when(
            F.col("gross_amount") >= ETLConstants.CATEGORY_HIGH_THRESHOLD,
            F.lit(SaleCategory.HIGH)
        ).when(
            F.col("gross_amount") >= ETLConstants.CATEGORY_MEDIUM_THRESHOLD,
            F.lit(SaleCategory.MEDIUM)
        ).otherwise(F.lit(SaleCategory.LOW))