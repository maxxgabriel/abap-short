"""
Data transformation module.

Handles transformation of raw sales data into analytics format
with business logic and calculations.
"""

from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType
)

from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLTransformer:
    """Transforms raw sales data into analytics format."""

    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the transformer.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.business_rules = config.get('business_rules', {})

    def transform_data(self, raw_data: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_data: DataFrame with raw sales data
            etl_run_id: ETL run identifier

        Returns:
            DataFrame with transformed analytics data
        """
        try:
            record_count = raw_data.count()

            self.logger.log_message(
                step=ETLConstants.STEP_TRANSFORM,
                status=ETLConstants.STATUS_SUCCESS,
                message=f"Starting transformation of {record_count} records"
            )

            # Add ETL run ID
            df = raw_data.withColumn("etl_run_id", F.lit(etl_run_id))

            # Calculate gross amount
            df = df.withColumn(
                "gross_amount",
                (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
            )

            # Calculate discount based on quantity tiers
            df = self._calculate_discount(df)

            # Calculate tax
            df = self._calculate_tax(df)

            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
                .cast(DecimalType(16, 2))
            )

            # Calculate profit margin
            df = self._calculate_profit_margin(df)

            # Categorize sales
            df = self._categorize_sales(df)

            # Generate analytics ID
            df = self._generate_analytics_id(df)

            # Select and rename columns to match analytics schema
            analytics_df = df.select(
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
                F.col("etl_run_id")
            )

            success_count = analytics_df.count()

            self.logger.log_message(
                step=ETLConstants.STEP_TRANSFORM,
                status=ETLConstants.STATUS_SUCCESS,
                records_processed=record_count,
                records_success=success_count,
                message=f"Transformed {success_count} of {record_count} records"
            )

            return analytics_df

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.STEP_TRANSFORM,
                status=ETLConstants.STATUS_ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            raise

    def _calculate_discount(self, df: DataFrame) -> DataFrame:
        """
        Calculate discount based on quantity thresholds.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with discount_amount column
        """
        tier1_qty = self.business_rules.get('discount_qty_tier1', 10)
        tier2_qty = self.business_rules.get('discount_qty_tier2', 15)
        tier1_rate = Decimal(str(self.business_rules.get('discount_rate_tier1', 0.05)))
        tier2_rate = Decimal(str(self.business_rules.get('discount_rate_tier2', 0.10)))

        return df.withColumn(
            "discount_amount",
            F.when(F.col("quantity") > tier2_qty,
                   F.col("gross_amount") * F.lit(tier2_rate))
            .when(F.col("quantity") > tier1_qty,
                  F.col("gross_amount") * F.lit(tier1_rate))
            .otherwise(F.lit(0))
            .cast(DecimalType(16, 2))
        )

    def _calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax amount.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with tax_amount column
        """
        tax_rate = Decimal(str(self.business_rules.get('tax_rate', 0.08)))

        return df.withColumn(
            "tax_amount",
            ((F.col("gross_amount") - F.col("discount_amount")) * F.lit(tax_rate))
            .cast(DecimalType(16, 2))
        )

    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with profit_margin column
        """
        cost_ratio = Decimal(str(self.business_rules.get('cost_ratio', 0.60)))

        df = df.withColumn(
            "cost_amount",
            (F.col("quantity") * F.col("unit_price") * F.lit(cost_ratio))
            .cast(DecimalType(16, 2))
        )

        return df.withColumn(
            "profit_margin",
            F.when(F.col("net_amount") > 0,
                   (((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
                   .cast(DecimalType(5, 2)))
            .otherwise(F.lit(0))
        )

    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with category column
        """
        high_threshold = Decimal(str(
            self.business_rules.get('category_high_threshold', 2000.00)
        ))
        medium_threshold = Decimal(str(
            self.business_rules.get('category_medium_threshold', 500.00)
        ))

        return df.withColumn(
            "category",
            F.when(F.col("gross_amount") >= high_threshold,
                   F.lit(ETLConstants.CATEGORY_HIGH))
            .when(F.col("gross_amount") >= medium_threshold,
                  F.lit(ETLConstants.CATEGORY_MEDIUM))
            .otherwise(F.lit(ETLConstants.CATEGORY_LOW))
        )

    def _generate_analytics_id(self, df: DataFrame) -> DataFrame:
        """
        Generate unique analytics ID.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with analytics_id column
        """
        return df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )