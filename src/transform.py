"""
Data transformation module for Sales ETL.
Transforms raw sales data into analytics format.
Migrated from ABAP ZCL_ETL_TRANSFORMER class.
"""

from decimal import Decimal
from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)

from src.logger import ETLLogger
from src.config import (
    ProcessStep, StatusCode, BusinessRules,
    SaleCategory, IDPrefix
)


class SalesTransformer:
    """Transforms raw sales data into analytics format."""
    
    # Schema for analytics data
    ANALYTICS_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def transform_data(
        self,
        raw_df: DataFrame,
        etl_run_id: str
    ) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame, or None if transformation fails
        """
        try:
            self.logger.log_message(
                step=ProcessStep.TRANSFORM,
                status=StatusCode.INFO,
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Register UDFs for business logic
            self._register_udfs()
            
            # Perform transformations
            analytics_df = (
                raw_df
                .withColumn("gross_amount", 
                    F.col("quantity") * F.col("unit_price"))
                .withColumn("discount_amount",
                    self._calculate_discount_udf(
                        F.col("quantity"),
                        F.col("gross_amount")
                    ))
                .withColumn("taxable_amount",
                    F.col("gross_amount") - F.col("discount_amount"))
                .withColumn("tax_amount",
                    F.col("taxable_amount") * F.lit(float(BusinessRules.TAX_RATE)))
                .withColumn("net_amount",
                    F.col("taxable_amount") + F.col("tax_amount"))
                .withColumn("profit_margin",
                    self._calculate_profit_margin_udf(
                        F.col("quantity"),
                        F.col("unit_price"),
                        F.col("net_amount")
                    ))
                .withColumn("category",
                    self._categorize_sale_udf(F.col("gross_amount")))
                .withColumn("analytics_id",
                    F.concat(
                        F.lit(IDPrefix.ANALYTICS_ID),
                        F.col("trans_id"),
                        F.date_format(F.current_timestamp(), "HHmmss")
                    ))
                .withColumn("etl_run_id", F.lit(etl_run_id))
                .select(
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
            )
            
            # Validate output
            analytics_df = self.spark.createDataFrame(
                analytics_df.rdd,
                schema=self.ANALYTICS_SCHEMA
            )
            
            final_count = analytics_df.count()
            
            self.logger.log_message(
                step=ProcessStep.TRANSFORM,
                status=StatusCode.SUCCESS,
                records_processed=initial_count,
                records_success=final_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.TRANSFORM,
                status=StatusCode.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return None
    
    def _register_udfs(self):
        """Register user-defined functions for transformations."""
        self._calculate_discount_udf = F.udf(
            self._calculate_discount,
            DecimalType(16, 2)
        )
        
        self._calculate_profit_margin_udf = F.udf(
            self._calculate_profit_margin,
            DecimalType(5, 2)
        )
        
        self._categorize_sale_udf = F.udf(
            self._categorize_sale,
            StringType()
        )
    
    @staticmethod
    def _calculate_discount(quantity: int, gross_amount: Decimal) -> Decimal:
        """
        Calculate discount based on quantity tiers.
        
        Args:
            quantity: Quantity ordered
            gross_amount: Gross sale amount
            
        Returns:
            Discount amount
        """
        if quantity > BusinessRules.DISCOUNT_QTY_TIER2:
            return gross_amount * BusinessRules.DISCOUNT_RATE_TIER2
        elif quantity > BusinessRules.DISCOUNT_QTY_TIER1:
            return gross_amount * BusinessRules.DISCOUNT_RATE_TIER1
        else:
            return Decimal('0.00')
    
    @staticmethod
    def _calculate_profit_margin(
        quantity: int,
        unit_price: Decimal,
        net_amount: Decimal
    ) -> Decimal:
        """
        Calculate profit margin percentage.
        
        Args:
            quantity: Quantity ordered
            unit_price: Unit price
            net_amount: Net sale amount
            
        Returns:
            Profit margin as percentage
        """
        cost = quantity * unit_price * BusinessRules.COST_RATIO
        if net_amount > 0:
            profit = net_amount - cost
            margin = (profit / net_amount) * Decimal('100')
            return margin.quantize(Decimal('0.01'))
        return Decimal('0.00')
    
    @staticmethod
    def _categorize_sale(gross_amount: Decimal) -> str:
        """
        Categorize sale based on gross amount.
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Sale category
        """
        if gross_amount >= BusinessRules.CATEGORY_HIGH_THRESHOLD:
            return SaleCategory.HIGH.value
        elif gross_amount >= BusinessRules.CATEGORY_MEDIUM_THRESHOLD:
            return SaleCategory.MEDIUM.value
        else:
            return SaleCategory.LOW.value