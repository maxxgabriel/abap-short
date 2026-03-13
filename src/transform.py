"""
PySpark Transformer Module
Migrated from ZCL_ETL_TRANSFORMER ABAP class
Transforms raw sales data into analytics format using DataFrame operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Tuple
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesTransformer:
    """
    Transforms raw sales data into analytics format.
    Equivalent to ZCL_ETL_TRANSFORMER ABAP class.
    """

    def __init__(self, logger: ETLLogger, config: ETLConfig):
        """
        Initialize transformer with logger and configuration.
        
        Args:
            logger: ETL logger instance
            config: ETL configuration instance
        """
        self.logger = logger
        self.config = config
        self.spark = SparkSession.getActiveSession()
        if not self.spark:
            raise RuntimeError("No active Spark session found")

    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        Equivalent to ty_analytics structure in ABAP.
        
        Returns:
            StructType schema for analytics DataFrame
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", StringType(), False)
        ])

    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        Replaces LOOP AT with DataFrame transformations.
        Equivalent to transform_data method in ABAP.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (analytics DataFrame, success boolean)
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )

            # Get initial count
            input_count = raw_df.count()
            
            # Cache input for reuse
            raw_df.cache()

            # Calculate analytics using DataFrame operations
            analytics_df = self._calculate_analytics(raw_df)

            # Get success count
            output_count = analytics_df.count()
            error_count = input_count - output_count

            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f'Transformed {output_count} of {input_count} records'
            )

            return analytics_df, True

        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            return None, False

    def _calculate_analytics(self, raw_df: DataFrame) -> DataFrame:
        """
        Calculate analytics metrics using DataFrame column expressions.
        Replaces calculate_analytics method from ABAP.
        Uses vectorized operations instead of row-by-row processing.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Analytics DataFrame with calculated fields
        """
        # Generate analytics ID with monotonically increasing ID
        analytics_df = raw_df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )

        # Calculate gross amount
        analytics_df = analytics_df.withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
        )

        # Calculate discount using CASE WHEN (replaces IF-ELSEIF from ABAP)
        analytics_df = analytics_df.withColumn(
            "discount_amount",
            F.when(F.col("quantity") > self.config.discount_qty_tier2,
                   F.col("gross_amount") * self.config.discount_rate_tier2)
            .when(F.col("quantity") > self.config.discount_qty_tier1,
                  F.col("gross_amount") * self.config.discount_rate_tier1)
            .otherwise(F.lit(0.0))
            .cast(DecimalType(16, 2))
        )

        # Calculate tax amount (8% on gross - discount)
        analytics_df = analytics_df.withColumn(
            "tax_amount",
            ((F.col("gross_amount") - F.col("discount_amount")) * self.config.tax_rate)
            .cast(DecimalType(16, 2))
        )

        # Calculate net amount
        analytics_df = analytics_df.withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
            .cast(DecimalType(16, 2))
        )

        # Calculate cost and profit margin
        analytics_df = analytics_df.withColumn(
            "cost_amount",
            (F.col("quantity") * F.col("unit_price") * self.config.cost_ratio)
            .cast(DecimalType(16, 2))
        )

        analytics_df = analytics_df.withColumn(
            "profit_margin",
            F.when(F.col("net_amount") > 0,
                   (((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
                   .cast(DecimalType(5, 2)))
            .otherwise(F.lit(0.0))
        )

        # Categorize sale using UDF-like CASE expression
        analytics_df = analytics_df.withColumn(
            "category",
            F.when(F.col("gross_amount") >= self.config.category_high_threshold,
                   F.lit("HIGH"))
            .when(F.col("gross_amount") >= self.config.category_medium_threshold,
                  F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
        )

        # Add ETL metadata
        analytics_df = analytics_df.withColumn(
            "etl_run_id",
            F.lit(self.logger.get_etl_run_id())
        )

        analytics_df = analytics_df.withColumn(
            "loaded_at",
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
        )

        # Rename quantity column
        analytics_df = analytics_df.withColumnRenamed("quantity", "total_quantity")

        # Select and order final columns
        final_columns = [
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
            "loaded_at"
        ]

        return analytics_df.select(*final_columns)

    def categorize_sale_udf(self, gross_amount: float) -> str:
        """
        Alternative UDF implementation for categorization.
        Can be registered as Spark UDF if preferred over SQL expressions.
        Equivalent to categorize_sale method in ABAP.
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Category string (HIGH, MEDIUM, LOW)
        """
        if gross_amount >= self.config.category_high_threshold:
            return "HIGH"
        elif gross_amount >= self.config.category_medium_threshold:
            return "MEDIUM"
        else:
            return "LOW"

    def validate_transformed_data(self, analytics_df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Validate transformed analytics data.
        Filters out invalid records.
        
        Args:
            analytics_df: Analytics DataFrame to validate
            
        Returns:
            Tuple of (valid DataFrame, error count)
        """
        initial_count = analytics_df.count()

        # Apply validation rules
        valid_df = analytics_df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )

        valid_count = valid_df.count()
        error_count = initial_count - valid_count

        if error_count > 0:
            self.logger.log_message(
                step='TRANSFORM',
                status='W',
                message=f'Filtered out {error_count} invalid records during validation'
            )

        return valid_df, error_count