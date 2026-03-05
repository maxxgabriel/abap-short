"""
ETL Transformer Module - Transforms raw sales data into analytics format
Converts ABAP ZCL_ETL_TRANSFORMER to PySpark operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import col, lit, when, current_timestamp, udf
from pyspark.sql.types import StringType as SparkStringType
from typing import Tuple
from decimal import Decimal

from src.logger import ETLLogger


class ETLTransformer:
    """
    Transforms raw sales data into analytics format (ABAP ZCL_ETL_TRANSFORMER equivalent).
    """

    # Business rules constants (from ABAP ZCL_ETL_CONSTANTS)
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal("0.05")
    DISCOUNT_RATE_TIER2 = Decimal("0.10")
    TAX_RATE = Decimal("0.08")
    COST_RATIO = Decimal("0.60")
    CATEGORY_HIGH_THRESHOLD = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD = Decimal("500.00")

    CATEGORY_HIGH = "HIGH"
    CATEGORY_MEDIUM = "MEDIUM"
    CATEGORY_LOW = "LOW"

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer.

        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.logger = logger
        self.config = config

        # Override constants from config if provided
        self.tax_rate = config.get('tax_rate', self.TAX_RATE)
        self.cost_ratio = config.get('cost_ratio', self.COST_RATIO)
        self.high_threshold = config.get('category_high_threshold', self.CATEGORY_HIGH_THRESHOLD)
        self.medium_threshold = config.get('category_medium_threshold', self.CATEGORY_MEDIUM_THRESHOLD)

    @staticmethod
    def get_analytics_schema() -> StructType:
        """
        Define schema for analytics data (ZSALES_ANALYTICS table mapping).

        Returns:
            StructType schema for analytics
        """
        return StructType([
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
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True)
        ])

    def transform_data(self, raw_df: DataFrame) -> Tuple[bool, DataFrame]:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_df: Raw sales DataFrame

        Returns:
            Tuple of (success_flag, analytics_dataframe)
        """
        try:
            self.logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_SUCCESS,
                message="Starting data transformation"
            )

            record_count = raw_df.count()

            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )

            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(col("quantity") > self.DISCOUNT_QTY_TIER2,
                     col("gross_amount") * lit(float(self.DISCOUNT_RATE_TIER2)))
                .when(col("quantity") > self.DISCOUNT_QTY_TIER1,
                      col("gross_amount") * lit(float(self.DISCOUNT_RATE_TIER1)))
                .otherwise(lit(0.0))
            )

            # Calculate tax (on gross - discount)
            df = df.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * lit(float(self.tax_rate))
            )

            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )

            # Calculate profit margin
            df = df.withColumn(
                "cost_amount",
                col("quantity") * col("unit_price") * lit(float(self.cost_ratio))
            )
            df = df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100.0))
                .otherwise(lit(0.0))
            )

            # Categorize sales
            df = df.withColumn(
                "category",
                when(col("gross_amount") >= lit(float(self.high_threshold)), lit(self.CATEGORY_HIGH))
                .when(col("gross_amount") >= lit(float(self.medium_threshold)), lit(self.CATEGORY_MEDIUM))
                .otherwise(lit(self.CATEGORY_LOW))
            )

            # Generate analytics ID
            from pyspark.sql.functions import concat, lit as spark_lit, date_format, monotonically_increasing_id
            df = df.withColumn(
                "analytics_id",
                concat(
                    spark_lit("ANL"),
                    col("trans_id"),
                    date_format(current_timestamp(), "HHmmss"),
                    (monotonically_increasing_id() % 1000000).cast("string")
                )
            )

            # Select and rename final columns
            analytics_df = df.select(
                col("analytics_id"),
                col("trans_date"),
                col("customer_id"),
                col("product_id"),
                col("quantity").alias("total_quantity"),
                col("gross_amount").cast("decimal(16,2)"),
                col("net_amount").cast("decimal(16,2)"),
                col("discount_amount").cast("decimal(16,2)"),
                col("tax_amount").cast("decimal(16,2)"),
                col("currency"),
                col("sales_rep"),
                col("region"),
                col("profit_margin").cast("decimal(5,2)"),
                col("category"),
                lit(self.logger.get_etl_run_id()).alias("etl_run_id"),
                current_timestamp().alias("loaded_at"),
                lit("etl_system").alias("loaded_by")
            )

            success_count = analytics_df.count()

            self.logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_SUCCESS,
                records_processed=record_count,
                records_success=success_count,
                message=f"Transformed {success_count} of {record_count} records"
            )

            return True, analytics_df

        except Exception as e:
            self.logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return False, self.spark.createDataFrame([], schema=self.get_analytics_schema())