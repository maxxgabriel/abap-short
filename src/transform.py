"""
Data transformation module for Sales ETL System.
Transforms raw sales data into analytics format with business calculations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from pyspark.sql.functions import col, when, lit, round as spark_round, concat, current_timestamp
from typing import Tuple
from decimal import Decimal

from src.logger import ETLLogger
from src.constants import ETLConstants


class DataTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the data transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.constants = ETLConstants()
    
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame with raw sales data
        
        Returns:
            Tuple of (DataFrame with transformed data, success flag)
        """
        try:
            self.logger.log_message(
                step=self.constants.STEP_TRANSFORM,
                status=self.constants.STATUS_INFO,
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._calculate_analytics(raw_df)
            
            output_count = transformed_df.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step=self.constants.STEP_TRANSFORM,
                status=self.constants.STATUS_SUCCESS,
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step=self.constants.STEP_TRANSFORM,
                status=self.constants.STATUS_ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return self.spark.createDataFrame([], self.get_analytics_schema()), False
    
    def _calculate_analytics(self, raw_df: DataFrame) -> DataFrame:
        """
        Apply business calculations to raw data.
        
        Args:
            raw_df: Raw sales DataFrame
        
        Returns:
            Transformed DataFrame with analytics
        """
        # Calculate gross amount
        df = raw_df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity
        df = df.withColumn(
            "discount_amount",
            when(
                col("quantity") > self.constants.DISCOUNT_QTY_TIER2,
                spark_round(col("gross_amount") * self.constants.DISCOUNT_RATE_TIER2, 2)
            ).when(
                col("quantity") > self.constants.DISCOUNT_QTY_TIER1,
                spark_round(col("gross_amount") * self.constants.DISCOUNT_RATE_TIER1, 2)
            ).otherwise(lit(Decimal("0.00")))
        )
        
        # Calculate tax (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * self.constants.TAX_RATE, 2)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            spark_round(
                col("gross_amount") - col("discount_amount") + col("tax_amount"),
                2
            )
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * self.constants.COST_RATIO, 2)
        )
        
        df = df.withColumn(
            "profit_margin",
            when(
                col("net_amount") > 0,
                spark_round(
                    ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                    2
                )
            ).otherwise(lit(Decimal("0.00")))
        )
        
        # Categorize sale
        df = df.withColumn(
            "category",
            when(
                col("gross_amount") >= self.constants.CATEGORY_HIGH_THRESHOLD,
                lit(self.constants.CATEGORY_HIGH)
            ).when(
                col("gross_amount") >= self.constants.CATEGORY_MEDIUM_THRESHOLD,
                lit(self.constants.CATEGORY_MEDIUM)
            ).otherwise(lit(self.constants.CATEGORY_LOW))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit(self.constants.PREFIX_ANALYTICS_ID),
                col("trans_id"),
                current_timestamp().cast("string")
            )
        )
        
        # Add ETL run ID
        df = df.withColumn(
            "etl_run_id",
            lit(self.logger.get_etl_run_id())
        )
        
        # Select and rename final columns
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
            col("etl_run_id")
        )
        
        return analytics_df