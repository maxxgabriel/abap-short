"""
ETL Transformer Module
Transforms raw sales data into analytics format
Migrated from ZCL_ETL_TRANSFORMER ABAP class
"""

from typing import Optional
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, udf, concat_ws, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger
from src.config import ETLConfig


class ETLTransformer:
    """
    Data transformation component for ETL pipeline.
    Applies business rules and calculations to raw sales data.
    """

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize transformer with logger and Spark session.
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark
        self.config = ETLConfig()

    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        Migrates ABAP ty_analytics structure to PySpark StructType.
        
        Returns:
            StructType schema for analytics data
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
            StructField("etl_run_id", StringType(), False)
        ])

    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        Migrates ABAP LOOP AT...ENDLOOP to PySpark DataFrame transformations.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data, or None if transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )

            input_count = raw_df.count()

            # Calculate gross amount (quantity * unit_price)
            df = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )

            # Calculate discount based on quantity tiers
            # Migrates ABAP IF-ELSEIF logic to PySpark when-otherwise
            df = df.withColumn(
                "discount_amount",
                when(col("quantity") > self.config.DISCOUNT_QTY_TIER2,
                     col("gross_amount") * lit(self.config.DISCOUNT_RATE_TIER2))
                .when(col("quantity") > self.config.DISCOUNT_QTY_TIER1,
                      col("gross_amount") * lit(self.config.DISCOUNT_RATE_TIER1))
                .otherwise(lit(0.0))
            )

            # Calculate tax (8% on gross - discount)
            df = df.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * lit(self.config.TAX_RATE)
            )

            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )

            # Calculate profit margin
            # Assume cost is 60% of unit price
            df = df.withColumn(
                "cost_amount",
                col("quantity") * col("unit_price") * lit(self.config.COST_RATIO)
            )

            df = df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     ((col("net_amount") - col("cost_amount")) / col("net_amount") * lit(100)))
                .otherwise(lit(0.0))
            )

            # Categorize sales (HIGH/MEDIUM/LOW)
            df = df.withColumn(
                "category",
                when(col("gross_amount") >= self.config.CATEGORY_HIGH_THRESHOLD, lit("HIGH"))
                .when(col("gross_amount") >= self.config.CATEGORY_MEDIUM_THRESHOLD, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )

            # Generate analytics_id
            df = df.withColumn(
                "analytics_id",
                concat_ws("_", lit("ANL"), col("trans_id"), 
                         current_timestamp().cast("string"))
            )

            # Add ETL run ID
            df = df.withColumn(
                "etl_run_id",
                lit(self.logger.etl_run_id)
            )

            # Select and rename columns to match analytics schema
            analytics_df = df.select(
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

            output_count = analytics_df.count()

            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )

            return analytics_df

        except Exception as ex:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(ex)}"
            )
            return None