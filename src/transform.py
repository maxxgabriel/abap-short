"""
PySpark Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from pyspark.sql.window import Window

from src.logger import ETLLogger
from src.exceptions import TransformError


class DataTransformer:
    """
    Handles data transformation with business logic and calculations.
    """

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize data transformer.

        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config

        # Business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)

    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_df: Raw sales DataFrame

        Returns:
            Transformed analytics DataFrame

        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )

            # Add ETL run ID
            etl_run_id = self.logger.get_etl_run_id()

            # Calculate analytics fields
            analytics_df = (
                raw_df
                .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
                .withColumn("discount_amount", self._calculate_discount())
                .withColumn("tax_amount", self._calculate_tax())
                .withColumn("net_amount", self._calculate_net_amount())
                .withColumn("profit_margin", self._calculate_profit_margin())
                .withColumn("category", self._categorize_sale())
                .withColumn("analytics_id", self._generate_analytics_id())
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

            record_count = analytics_df.count()

            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message=f'Transformed {record_count} records successfully',
                records_processed=record_count,
                records_success=record_count
            )

            return analytics_df

        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            raise TransformError(f"Data transformation failed: {str(e)}")

    def _calculate_discount(self):
        """Calculate discount based on quantity tiers."""
        return F.when(
            F.col("quantity") > self.discount_qty_tier2,
            F.col("quantity") * F.col("unit_price") * self.discount_rate_tier2
        ).when(
            F.col("quantity") > self.discount_qty_tier1,
            F.col("quantity") * F.col("unit_price") * self.discount_rate_tier1
        ).otherwise(0.0)

    def _calculate_tax(self):
        """Calculate tax on gross amount minus discount."""
        return (F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate

    def _calculate_net_amount(self):
        """Calculate final net amount."""
        return F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")

    def _calculate_profit_margin(self):
        """Calculate profit margin percentage."""
        cost = F.col("quantity") * F.col("unit_price") * self.cost_ratio
        return ((F.col("net_amount") - cost) / F.col("net_amount")) * 100

    def _categorize_sale(self):
        """Categorize sales based on gross amount."""
        return F.when(
            F.col("gross_amount") >= self.category_high_threshold,
            F.lit("HIGH")
        ).when(
            F.col("gross_amount") >= self.category_medium_threshold,
            F.lit("MEDIUM")
        ).otherwise(F.lit("LOW"))

    def _generate_analytics_id(self):
        """Generate unique analytics ID."""
        return F.concat(
            F.lit("ANL_"),
            F.col("trans_id"),
            F.lit("_"),
            F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
        )