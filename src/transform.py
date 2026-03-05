"""
Transform Module
Handles transformation of raw sales data into analytics format.
"""

from datetime import datetime
from typing import Optional
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, concat, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.exceptions import TransformError


class SalesTransformer:
    """
    Transforms raw sales data into analytics format with business calculations.
    """

    # Schema for analytics data
    ANALYTICS_SCHEMA = StructType([
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

    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize the transformer.

        Args:
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
        self.stats = {
            "records_processed": 0,
            "records_success": 0,
            "records_error": 0
        }

        # Load business rules from config
        rules = config.get("business_rules", {})
        self.discount_qty_tier1 = rules.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = rules.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = rules.get("discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = rules.get("discount_rate_tier2", 0.10)
        self.tax_rate = rules.get("tax_rate", 0.08)
        self.cost_ratio = rules.get("cost_ratio", 0.60)
        self.category_high_threshold = rules.get("category_high_threshold", 2000.00)
        self.category_medium_threshold = rules.get("category_medium_threshold", 500.00)

    def transform_data(self, raw_data: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_data: Raw sales DataFrame

        Returns:
            Transformed analytics DataFrame

        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )

            initial_count = raw_data.count()
            self.stats["records_processed"] = initial_count

            # Apply transformations
            analytics_data = self._apply_transformations(raw_data)

            # Validate transformed data
            self._validate_transformed_data(analytics_data)

            final_count = analytics_data.count()
            error_count = initial_count - final_count
            self.stats["records_success"] = final_count
            self.stats["records_error"] = error_count

            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )

            return analytics_data

        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            logging.error(f"Transformation error: {str(e)}", exc_info=True)
            raise TransformError(f"Failed to transform data: {str(e)}") from e

    def _apply_transformations(self, raw_data: DataFrame) -> DataFrame:
        """
        Apply all business transformations.

        Args:
            raw_data: Raw sales DataFrame

        Returns:
            Transformed DataFrame
        """
        # Calculate gross amount
        df = raw_data.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > self.discount_qty_tier2,
                 spark_round(col("gross_amount") * self.discount_rate_tier2, 2))
            .when(col("quantity") > self.discount_qty_tier1,
                  spark_round(col("gross_amount") * self.discount_rate_tier1, 2))
            .otherwise(lit(0.0))
        )

        # Calculate tax amount (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * self.tax_rate, 2)
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
            spark_round(col("quantity") * col("unit_price") * self.cost_ratio, 2)
        )

        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                     2
                 ))
            .otherwise(lit(0.0))
        )

        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= self.category_high_threshold, lit("HIGH"))
            .when(col("gross_amount") >= self.category_medium_threshold, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )

        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                lit("_"),
                col("trans_date").cast("string")
            )
        )

        # Add ETL run ID
        df = df.withColumn(
            "etl_run_id",
            lit(self.logger.get_etl_run_id())
        )

        # Select final columns in schema order
        result_df = df.select(
            "analytics_id",
            "trans_date",
            "customer_id",
            "product_id",
            col("quantity").alias("total_quantity"),
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

        return result_df

    def _validate_transformed_data(self, df: DataFrame) -> None:
        """
        Validate transformed data.

        Args:
            df: Transformed DataFrame

        Raises:
            TransformError: If validation fails
        """
        # Check for negative amounts
        negative_check = df.filter(
            (col("gross_amount") < 0) |
            (col("net_amount") < 0)
        ).count()

        if negative_check > 0:
            raise TransformError(f"Found {negative_check} records with negative amounts")

        # Check for invalid categories
        invalid_categories = df.filter(
            ~col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()

        if invalid_categories > 0:
            raise TransformError(f"Found {invalid_categories} records with invalid categories")

    def get_stats(self) -> dict:
        """Get transformation statistics."""
        return self.stats.copy()