"""
ETL Transformer module.
Converted from ABAP ZCL_ETL_TRANSFORMER class.
"""

from decimal import Decimal
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, expr, concat, date_format, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)

from src.config import CONSTANTS, ETLStep, ETLStatus, SaleCategory
from src.logger import ETLLogger


class ETLTransformer:
    """
    Data transformation component for ETL process.
    Transforms raw sales data into analytics format.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.schema = self._get_analytics_schema()
        self.business_rules = CONSTANTS.business_rules
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[bool, DataFrame]:
        """
        Transform raw sales data to analytics format.
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            Tuple of (success flag, DataFrame with analytics data)
        """
        try:
            self.logger.log_message(
                step=ETLStep.TRANSFORM,
                status=ETLStatus.SUCCESS,
                message=CONSTANTS.messages.TRANSFORM_START
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations
            analytics_df = self._calculate_analytics(raw_df)
            
            # Validate transformed records
            analytics_df = analytics_df.filter(
                (col("gross_amount") > 0) &
                (col("analytics_id").isNotNull())
            )
            
            final_count = analytics_df.count()
            error_count = initial_count - final_count
            
            self.logger.log_message(
                step=ETLStep.TRANSFORM,
                status=ETLStatus.SUCCESS,
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return True, analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step=ETLStep.TRANSFORM,
                status=ETLStatus.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return False, self.spark.createDataFrame([], self.schema)
    
    def _calculate_analytics(self, raw_df: DataFrame) -> DataFrame:
        """
        Calculate analytics metrics from raw data.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            DataFrame with calculated analytics
        """
        # Generate analytics ID
        analytics_df = raw_df.withColumn(
            "analytics_id",
            concat(
                lit(CONSTANTS.config.PREFIX_ANALYTICS_ID),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmss")
            )
        )
        
        # Calculate gross amount
        analytics_df = analytics_df.withColumn(
            "gross_amount",
            col("quantity") * col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        analytics_df = analytics_df.withColumn(
            "discount_amount",
            when(
                col("quantity") > self.business_rules.DISCOUNT_QTY_TIER2,
                col("gross_amount") * lit(float(self.business_rules.DISCOUNT_RATE_TIER2))
            ).when(
                col("quantity") > self.business_rules.DISCOUNT_QTY_TIER1,
                col("gross_amount") * lit(float(self.business_rules.DISCOUNT_RATE_TIER1))
            ).otherwise(lit(Decimal("0.00")))
        )
        
        # Calculate tax (on gross - discount)
        analytics_df = analytics_df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * 
            lit(float(self.business_rules.TAX_RATE))
        )
        
        # Calculate net amount
        analytics_df = analytics_df.withColumn(
            "net_amount",
            col("gross_amount") - col("discount_amount") + col("tax_amount")
        )
        
        # Calculate profit margin
        # Profit margin = ((Net - Cost) / Net) * 100
        # Cost = Quantity * Unit Price * Cost Ratio
        analytics_df = analytics_df.withColumn(
            "cost_amount",
            col("quantity") * col("unit_price") * 
            lit(float(self.business_rules.COST_RATIO))
        )
        
        analytics_df = analytics_df.withColumn(
            "profit_margin",
            when(
                col("net_amount") > 0,
                ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100)
            ).otherwise(lit(Decimal("0.00")))
        )
        
        # Categorize sale
        analytics_df = analytics_df.withColumn(
            "category",
            when(
                col("gross_amount") >= lit(float(self.business_rules.CATEGORY_HIGH_THRESHOLD)),
                lit(SaleCategory.HIGH.value)
            ).when(
                col("gross_amount") >= lit(float(self.business_rules.CATEGORY_MEDIUM_THRESHOLD)),
                lit(SaleCategory.MEDIUM.value)
            ).otherwise(lit(SaleCategory.LOW.value))
        )
        
        # Add ETL metadata
        analytics_df = analytics_df.withColumn(
            "etl_run_id",
            lit(self.logger.get_etl_run_id())
        ).withColumn(
            "loaded_at",
            current_timestamp()
        ).withColumn(
            "loaded_by",
            lit("ETL_SYSTEM")
        )
        
        # Rename quantity to total_quantity for clarity
        analytics_df = analytics_df.withColumnRenamed("quantity", "total_quantity")
        
        # Select final columns in correct order
        analytics_df = analytics_df.select(
            "analytics_id",
            "trans_date",
            "customer_id",
            "product_id",
            "total_quantity",
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
        
        return analytics_df
    
    def _get_analytics_schema(self) -> StructType:
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), True),
            StructField("loaded_by", StringType(), True)
        ])