"""
Data transformation component for ETL system.
Converted from ABAP ZCL_ETL_TRANSFORMER.
"""

from decimal import Decimal
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, udf, concat_ws, 
    date_format, unix_timestamp
)
from pyspark.sql.types import StringType, DecimalType
from src.logger import ETLLogger
from src.constants import ETLConstants
from src.schemas import ETLSchemas


class ETLTransformer:
    """Transforms raw sales data into analytics format."""

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize transformer with logger and Spark session.

        Args:
            logger: ETL logger instance
            spark: Active SparkSession
        """
        self.logger = logger
        self.spark = spark

    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data to analytics format.

        Args:
            raw_df: Raw sales DataFrame

        Returns:
            Transformed analytics DataFrame, or None if transformation fails
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.SUCCESS,
                message="Starting data transformation",
            )

            initial_count = raw_df.count()

            # Calculate analytics fields
            analytics_df = (
                raw_df
                # Calculate gross amount
                .withColumn(
                    "gross_amount",
                    col("quantity") * col("unit_price")
                )
                # Calculate discount based on quantity
                .withColumn(
                    "discount_amount",
                    when(
                        col("quantity") > ETLConstants.DISCOUNT_QTY_TIER2,
                        col("gross_amount") * lit(ETLConstants.DISCOUNT_RATE_TIER2)
                    ).when(
                        col("quantity") > ETLConstants.DISCOUNT_QTY_TIER1,
                        col("gross_amount") * lit(ETLConstants.DISCOUNT_RATE_TIER1)
                    ).otherwise(lit(0.0))
                )
                # Calculate tax on discounted amount
                .withColumn(
                    "tax_amount",
                    (col("gross_amount") - col("discount_amount")) * lit(ETLConstants.TAX_RATE)
                )
                # Calculate net amount
                .withColumn(
                    "net_amount",
                    col("gross_amount") - col("discount_amount") + col("tax_amount")
                )
                # Calculate profit margin
                .withColumn(
                    "cost_amount",
                    col("quantity") * col("unit_price") * lit(ETLConstants.COST_RATIO)
                )
                .withColumn(
                    "profit_margin",
                    when(
                        col("net_amount") > 0,
                        ((col("net_amount") - col("cost_amount")) / col("net_amount") * 100)
                    ).otherwise(lit(0.0))
                )
                # Categorize sales
                .withColumn(
                    "category",
                    when(
                        col("gross_amount") >= ETLConstants.CATEGORY_HIGH_THRESHOLD,
                        lit(ETLConstants.Category.HIGH)
                    ).when(
                        col("gross_amount") >= ETLConstants.CATEGORY_MEDIUM_THRESHOLD,
                        lit(ETLConstants.Category.MEDIUM)
                    ).otherwise(lit(ETLConstants.Category.LOW))
                )
                # Generate analytics ID
                .withColumn(
                    "analytics_id",
                    concat_ws(
                        "",
                        lit(ETLConstants.PREFIX_ANALYTICS_ID),
                        col("trans_id"),
                        date_format(col("trans_date"), "yyyyMMdd")
                    )
                )
                # Add ETL metadata
                .withColumn("etl_run_id", lit(self.logger.get_etl_run_id()))
                .withColumn("loaded_at", lit(datetime.now()))
                .withColumn("loaded_by", lit("PYSPARK_ETL"))
                .withColumn("total_quantity", col("quantity"))
                # Select and rename columns to match analytics schema
                .select(
                    "analytics_id",
                    col("trans_date"),
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
            )

            # Validate transformed data
            final_count = analytics_df.count()
            error_count = initial_count - final_count

            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.SUCCESS,
                message=f"Transformed {final_count} of {initial_count} records",
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
            )

            return analytics_df

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.ERROR,
                message=f"Transformation failed: {str(e)}",
            )
            return None