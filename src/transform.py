"""
Data transformation component for Sales ETL system.
Converted from ABAP ZCL_ETL_TRANSFORMER.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.constants import ETLConstants
from src.logger import ETLLogger
from src.schemas import ETLSchemas, ProcessSteps, SaleCategories, StatusCodes


class ETLTransformer:
    """Transforms raw sales data into analytics format."""

    def __init__(self, logger: ETLLogger, etl_run_id: str):
        """
        Initialize transformer.
        
        Args:
            logger: ETL logger instance
            etl_run_id: Current ETL run identifier
        """
        self.logger = logger
        self.etl_run_id = etl_run_id

    def transform_data(self, df_raw: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data to analytics format.
        
        Args:
            df_raw: DataFrame with raw sales data
            
        Returns:
            DataFrame with analytics data, or None on failure
        """
        try:
            self.logger.log_message(
                step=ProcessSteps.TRANSFORM,
                status=StatusCodes.SUCCESS,
                message="Starting data transformation",
            )

            record_count = df_raw.count()

            # Calculate derived fields
            df_transformed = (
                df_raw
                .withColumn("gross_amount", 
                           F.col("quantity") * F.col("unit_price"))
                .withColumn("discount_amount", 
                           self._calculate_discount(
                               F.col("quantity"), 
                               F.col("gross_amount")
                           ))
                .withColumn("taxable_amount",
                           F.col("gross_amount") - F.col("discount_amount"))
                .withColumn("tax_amount",
                           F.col("taxable_amount") * F.lit(ETLConstants.TAX_RATE))
                .withColumn("net_amount",
                           F.col("taxable_amount") + F.col("tax_amount"))
                .withColumn("cost_amount",
                           F.col("quantity") * F.col("unit_price") * 
                           F.lit(ETLConstants.COST_RATIO))
                .withColumn("profit_margin",
                           F.when(F.col("net_amount") > 0,
                                 ((F.col("net_amount") - F.col("cost_amount")) / 
                                  F.col("net_amount") * 100))
                            .otherwise(F.lit(0)))
                .withColumn("category",
                           self._categorize_sale(F.col("gross_amount")))
                .withColumn("analytics_id",
                           self._generate_analytics_id(F.col("trans_id")))
                .withColumn("etl_run_id", F.lit(self.etl_run_id))
                .withColumn("loaded_at", F.current_timestamp())
                .withColumn("loaded_by", F.lit("SYSTEM"))
            )

            # Select final columns matching analytics schema
            df_analytics = df_transformed.select(
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
                "etl_run_id",
                "loaded_at",
                "loaded_by",
            )

            success_count = df_analytics.count()

            self.logger.log_message(
                step=ProcessSteps.TRANSFORM,
                status=StatusCodes.SUCCESS,
                records_processed=record_count,
                records_success=success_count,
                message=f"Transformed {success_count} of {record_count} records",
            )

            return df_analytics

        except Exception as e:
            self.logger.log_message(
                step=ProcessSteps.TRANSFORM,
                status=StatusCodes.ERROR,
                message=f"Transformation failed: {str(e)}",
            )
            return None

    def _calculate_discount(self, quantity_col, gross_amount_col):
        """
        Calculate discount based on quantity tiers.
        
        Args:
            quantity_col: Quantity column
            gross_amount_col: Gross amount column
            
        Returns:
            Column expression for discount amount
        """
        return F.when(
            quantity_col > ETLConstants.DISCOUNT_QTY_TIER2,
            gross_amount_col * F.lit(ETLConstants.DISCOUNT_RATE_TIER2)
        ).when(
            quantity_col > ETLConstants.DISCOUNT_QTY_TIER1,
            gross_amount_col * F.lit(ETLConstants.DISCOUNT_RATE_TIER1)
        ).otherwise(F.lit(0))

    def _categorize_sale(self, gross_amount_col):
        """
        Categorize sale based on gross amount.
        
        Args:
            gross_amount_col: Gross amount column
            
        Returns:
            Column expression for category
        """
        return F.when(
            gross_amount_col >= ETLConstants.CATEGORY_HIGH_THRESHOLD,
            F.lit(SaleCategories.HIGH)
        ).when(
            gross_amount_col >= ETLConstants.CATEGORY_MEDIUM_THRESHOLD,
            F.lit(SaleCategories.MEDIUM)
        ).otherwise(F.lit(SaleCategories.LOW))

    def _generate_analytics_id(self, trans_id_col):
        """
        Generate unique analytics ID.
        
        Args:
            trans_id_col: Transaction ID column
            
        Returns:
            Column expression for analytics ID
        """
        # Concatenate prefix + trans_id + timestamp component
        timestamp_part = F.date_format(F.current_timestamp(), "HHmmss")
        return F.concat(
            F.lit(ETLConstants.PREFIX_ANALYTICS_ID),
            trans_id_col,
            timestamp_part
        )