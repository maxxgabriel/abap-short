"""
ETL Transformer Component

Transforms raw sales data into analytics format with business rules.
Converted from ABAP ZCL_ETL_TRANSFORMER class.
"""

from typing import Tuple
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, date_format, round as spark_round
)

from src.schemas import ETLSchemas
from src.constants import ETLConstants
from src.logger import ETLLogger


class ETLTransformer:
    """Transforms raw sales data into analytics format"""

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.schema = ETLSchemas.analytics_schema()

    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data to analytics format
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            Tuple of (transformed DataFrame, success flag)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.INFO,
                message="Starting data transformation"
            )

            initial_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._calculate_analytics(raw_df)
            
            # Validate results
            transformed_df = self._validate_transformed_data(transformed_df)
            
            final_count = transformed_df.count()
            error_count = initial_count - final_count
            
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.SUCCESS,
                message=f"Transformed {final_count} of {initial_count} records",
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count
            )
            
            return transformed_df, True

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return self.spark.createDataFrame([], self.schema), False

    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """Apply all analytical calculations"""
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(
                col("quantity") > ETLConstants.DISCOUNT_QTY_TIER2,
                spark_round(col("gross_amount") * lit(ETLConstants.DISCOUNT_RATE_TIER2), 2)
            ).when(
                col("quantity") > ETLConstants.DISCOUNT_QTY_TIER1,
                spark_round(col("gross_amount") * lit(ETLConstants.DISCOUNT_RATE_TIER1), 2)
            ).otherwise(lit(0.0))
        )
        
        # Calculate tax amount (after discount)
        df = df.withColumn(
            "tax_amount",
            spark_round(
                (col("gross_amount") - col("discount_amount")) * lit(ETLConstants.TAX_RATE),
                2
            )
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
            spark_round(
                col("quantity") * col("unit_price") * lit(ETLConstants.COST_RATIO),
                2
            )
        )
        
        df = df.withColumn(
            "profit_margin",
            when(
                col("net_amount") > 0,
                spark_round(
                    ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                    2
                )
            ).otherwise(lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
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
        df = df.withColumn(
            "analytics_id",
            concat(
                lit(ETLConstants.PREFIX_ANALYTICS_ID),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmss")
            )
        )
        
        # Add metadata
        df = df.withColumn("etl_run_id", lit(self.logger.get_etl_run_id()))
        df = df.withColumn("loaded_at", current_timestamp())
        df = df.withColumn("loaded_by", lit("etl_system"))
        
        # Rename and select final columns
        df = df.withColumn("total_quantity", col("quantity"))
        
        # Select columns matching analytics schema
        return df.select(
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
            "loaded_at",
            "loaded_by"
        )

    def _validate_transformed_data(self, df: DataFrame) -> DataFrame:
        """Validate transformed records and filter invalid ones"""
        # Filter out records with invalid calculations
        valid_df = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0) &
            (col("net_amount") > 0) &
            col("category").isin(
                ETLConstants.Category.HIGH,
                ETLConstants.Category.MEDIUM,
                ETLConstants.Category.LOW
            )
        )
        
        return valid_df

    def calculate_aggregates(self, analytics_df: DataFrame) -> DataFrame:
        """
        Calculate aggregated statistics by category
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            DataFrame with aggregated statistics
        """
        return analytics_df.groupBy("category").agg({
            "total_quantity": "sum",
            "gross_amount": "sum",
            "net_amount": "sum",
            "discount_amount": "sum",
            "tax_amount": "sum",
            "analytics_id": "count"
        }).withColumnRenamed("count(analytics_id)", "record_count")