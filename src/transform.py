"""
Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp,
    round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import TransformError


class SalesTransformer:
    """Transforms raw sales data applying business rules."""

    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the transformer.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger

    def transform_data(self, raw_data: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.

        Args:
            raw_data: Raw sales DataFrame

        Returns:
            DataFrame containing analytics data

        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="I",
                message="Starting data transformation"
            )

            input_count = raw_data.count()

            # Apply business rules
            df_transformed = self._apply_business_rules(raw_data)

            # Add metadata
            df_final = self._add_metadata(df_transformed)

            output_count = df_final.count()

            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )

            return df_final

        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(f"Failed to transform data: {str(e)}") from e

    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformation rules.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        # Get business rule parameters from config
        discount_tier1_qty = self.config.get("discount_qty_tier1", 10)
        discount_tier2_qty = self.config.get("discount_qty_tier2", 15)
        discount_rate_tier1 = self.config.get("discount_rate_tier1", 0.05)
        discount_rate_tier2 = self.config.get("discount_rate_tier2", 0.10)
        tax_rate = self.config.get("tax_rate", 0.08)
        cost_ratio = self.config.get("cost_ratio", 0.60)
        category_high_threshold = self.config.get("category_high_threshold", 2000.00)
        category_medium_threshold = self.config.get("category_medium_threshold", 500.00)

        # Calculate gross amount
        df = df.withColumn("gross_amount", col("quantity") * col("unit_price"))

        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_tier2_qty, 
                 col("gross_amount") * discount_rate_tier2)
            .when(col("quantity") > discount_tier1_qty, 
                  col("gross_amount") * discount_rate_tier1)
            .otherwise(0.0)
        )

        # Calculate tax on (gross - discount)
        df = df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * tax_rate
        )

        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            col("gross_amount") - col("discount_amount") + col("tax_amount")
        )

        # Calculate profit margin (cost is assumed as cost_ratio of unit price)
        df = df.withColumn(
            "estimated_cost",
            col("quantity") * col("unit_price") * cost_ratio
        )
        
        df = df.withColumn(
            "profit_margin",
            spark_round(
                ((col("net_amount") - col("estimated_cost")) / col("net_amount")) * 100,
                2
            )
        )

        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= category_high_threshold, "HIGH")
            .when(col("gross_amount") >= category_medium_threshold, "MEDIUM")
            .otherwise("LOW")
        )

        # Generate analytics_id
        df = df.withColumn(
            "analytics_id",
            concat(lit("ANL"), col("trans_id"))
        )

        # Select and rename columns for final output
        df = df.select(
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
            col("category")
        )

        return df

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with metadata columns
        """
        run_id = self.logger.get_run_id()
        
        df = df.withColumn("etl_run_id", lit(run_id))
        df = df.withColumn("loaded_at", current_timestamp())

        return df

    def _get_analytics_schema(self) -> StructType:
        """
        Define the schema for analytics data.

        Returns:
            StructType: Schema definition
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
            StructField("etl_run_id", StringType(), nullable=False)
        ])