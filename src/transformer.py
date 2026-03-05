"""
ETL Transformer Module
Transforms raw sales data into analytics format with business rules.
"""

from typing import Dict, Any
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.exceptions import TransformError


class ETLTransformer:
    """
    Transforms raw sales data into analytics format applying business rules.
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        config: Dict[str, Any]
    ):
        """
        Initialize the transformer.

        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Transformer configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.statistics = {
            'records_transformed': 0,
            'records_failed': 0
        }

        # Business rule constants
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = Decimal(config.get('discount_rate_tier1', '0.05'))
        self.discount_rate_tier2 = Decimal(config.get('discount_rate_tier2', '0.10'))
        self.tax_rate = Decimal(config.get('tax_rate', '0.08'))
        self.cost_ratio = Decimal(config.get('cost_ratio', '0.60'))
        self.category_high_threshold = Decimal(config.get('category_high_threshold', '2000.00'))
        self.category_medium_threshold = Decimal(config.get('category_medium_threshold', '500.00'))

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
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )

            # Calculate analytics fields
            analytics_data = self._calculate_analytics(raw_data)

            # Validate transformed data
            analytics_data = self._validate_records(analytics_data)

            # Cache for performance
            analytics_data.cache()

            success_count = analytics_data.count()
            total_count = raw_data.count()
            error_count = total_count - success_count

            self.statistics['records_transformed'] = success_count
            self.statistics['records_failed'] = error_count

            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f'Transformed {success_count} of {total_count} records'
            )

            return analytics_data

        except Exception as e:
            raise TransformError(
                error_text=f"Transformation failed: {str(e)}",
                error_step='TRANSFORM'
            )

    def _calculate_analytics(self, raw_data: DataFrame) -> DataFrame:
        """
        Apply business rules and calculate analytics fields.

        Args:
            raw_data: Raw sales DataFrame

        Returns:
            DataFrame with calculated analytics fields
        """
        # Generate analytics ID
        analytics_data = raw_data.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL_"),
                F.col("trans_id"),
                F.lit("_"),
                F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
            )
        )

        # Calculate gross amount
        analytics_data = analytics_data.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )

        # Calculate discount based on quantity tiers
        analytics_data = analytics_data.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.discount_qty_tier2,
                F.col("gross_amount") * F.lit(float(self.discount_rate_tier2))
            ).when(
                F.col("quantity") > self.discount_qty_tier1,
                F.col("gross_amount") * F.lit(float(self.discount_rate_tier1))
            ).otherwise(F.lit(0.0))
        )

        # Calculate tax on (gross - discount)
        analytics_data = analytics_data.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * F.lit(float(self.tax_rate))
        )

        # Calculate net amount (gross - discount + tax)
        analytics_data = analytics_data.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )

        # Calculate cost (simplified: cost_ratio * gross)
        analytics_data = analytics_data.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(float(self.cost_ratio))
        )

        # Calculate profit margin
        analytics_data = analytics_data.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0.0))
        )

        # Categorize sales
        analytics_data = analytics_data.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= float(self.category_high_threshold),
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= float(self.category_medium_threshold),
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )

        # Add ETL run ID
        analytics_data = analytics_data.withColumn(
            "etl_run_id",
            F.lit(self.logger.get_etl_run_id())
        )

        # Add loaded timestamp
        analytics_data = analytics_data.withColumn(
            "loaded_at",
            F.current_timestamp()
        )

        # Select and rename columns
        analytics_data = analytics_data.select(
            F.col("analytics_id"),
            F.col("trans_date"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("quantity").alias("total_quantity"),
            F.col("gross_amount"),
            F.col("net_amount"),
            F.col("discount_amount"),
            F.col("tax_amount"),
            F.col("currency"),
            F.col("sales_rep"),
            F.col("region"),
            F.col("profit_margin"),
            F.col("category"),
            F.col("etl_run_id"),
            F.col("loaded_at")
        )

        return analytics_data

    def _validate_records(self, analytics_data: DataFrame) -> DataFrame:
        """
        Validate transformed records.

        Args:
            analytics_data: Analytics DataFrame

        Returns:
            DataFrame with only valid records
        """
        # Filter out invalid records
        valid_data = analytics_data.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin("HIGH", "MEDIUM", "LOW"))
        )

        return valid_data

    def get_statistics(self) -> Dict[str, int]:
        """
        Get transformation statistics.

        Returns:
            Dictionary with statistics
        """
        return self.statistics.copy()