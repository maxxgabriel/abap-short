"""
ETL Transform Module
Transforms raw sales data with business logic and logging
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger, ETLStep, ETLStatus


class SalesDataTransformer:
    """Transform raw sales data into analytics format"""

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize transformer

        Args:
            logger: ETL logger instance
            spark: SparkSession instance
        """
        self.logger = logger
        self.spark = spark

        # Business rules from config
        self.discount_qty_tier1 = 10
        self.discount_qty_tier2 = 15
        self.discount_rate_tier1 = 0.05
        self.discount_rate_tier2 = 0.10
        self.tax_rate = 0.08
        self.cost_ratio = 0.60
        self.category_high_threshold = 2000.00
        self.category_medium_threshold = 500.00

    def get_schema(self) -> StructType:
        """Define schema for analytics data"""
        return StructType([
            StructField('analytics_id', StringType(), False),
            StructField('trans_date', DateType(), False),
            StructField('customer_id', StringType(), False),
            StructField('product_id', StringType(), False),
            StructField('total_quantity', IntegerType(), False),
            StructField('gross_amount', DecimalType(16, 2), False),
            StructField('net_amount', DecimalType(16, 2), False),
            StructField('discount_amount', DecimalType(16, 2), False),
            StructField('tax_amount', DecimalType(16, 2), False),
            StructField('currency', StringType(), False),
            StructField('sales_rep', StringType(), True),
            StructField('region', StringType(), True),
            StructField('profit_margin', DecimalType(5, 2), False),
            StructField('category', StringType(), False),
            StructField('etl_run_id', StringType(), False)
        ])

    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data with business logic

        Args:
            raw_df: Raw sales DataFrame

        Returns:
            Transformed analytics DataFrame

        Raises:
            Exception: If transformation fails
        """
        try:
            self.logger.log_info(
                ETLStep.TRANSFORM,
                "Starting data transformation"
            )

            initial_count = raw_df.count()

            # Calculate gross amount
            df = raw_df.withColumn(
                'gross_amount',
                F.col('quantity') * F.col('unit_price')
            )

            # Calculate discount based on quantity tiers
            df = df.withColumn(
                'discount_amount',
                F.when(
                    F.col('quantity') > self.discount_qty_tier2,
                    F.col('gross_amount') * F.lit(self.discount_rate_tier2)
                ).when(
                    F.col('quantity') > self.discount_qty_tier1,
                    F.col('gross_amount') * F.lit(self.discount_rate_tier1)
                ).otherwise(F.lit(0.0))
            )

            # Calculate tax
            df = df.withColumn(
                'tax_amount',
                (F.col('gross_amount') - F.col('discount_amount')) * F.lit(self.tax_rate)
            )

            # Calculate net amount
            df = df.withColumn(
                'net_amount',
                F.col('gross_amount') - F.col('discount_amount') + F.col('tax_amount')
            )

            # Calculate cost and profit margin
            df = df.withColumn(
                'cost',
                F.col('quantity') * F.col('unit_price') * F.lit(self.cost_ratio)
            ).withColumn(
                'profit_margin',
                F.when(
                    F.col('net_amount') > 0,
                    ((F.col('net_amount') - F.col('cost')) / F.col('net_amount')) * 100
                ).otherwise(F.lit(0.0))
            )

            # Categorize sales
            df = df.withColumn(
                'category',
                F.when(
                    F.col('gross_amount') >= self.category_high_threshold,
                    F.lit('HIGH')
                ).when(
                    F.col('gross_amount') >= self.category_medium_threshold,
                    F.lit('MEDIUM')
                ).otherwise(F.lit('LOW'))
            )

            # Generate analytics ID
            df = df.withColumn(
                'analytics_id',
                F.concat(
                    F.lit('ANL'),
                    F.col('trans_id'),
                    F.date_format(F.current_timestamp(), 'HHmmss')
                )
            )

            # Add ETL run ID
            df = df.withColumn(
                'etl_run_id',
                F.lit(self.logger.get_etl_run_id())
            )

            # Select and rename columns for final output
            analytics_df = df.select(
                'analytics_id',
                'trans_date',
                'customer_id',
                'product_id',
                F.col('quantity').alias('total_quantity'),
                'gross_amount',
                'net_amount',
                'discount_amount',
                'tax_amount',
                'currency',
                'sales_rep',
                'region',
                'profit_margin',
                'category',
                'etl_run_id'
            )

            final_count = analytics_df.count()

            # Log any discrepancies
            if final_count != initial_count:
                self.logger.log_warning(
                    ETLStep.TRANSFORM,
                    f"Record count changed: {initial_count} -> {final_count}",
                    records_processed=initial_count,
                    records_success=final_count,
                    records_error=initial_count - final_count
                )
            else:
                self.logger.log_success(
                    ETLStep.TRANSFORM,
                    f"Transformed {final_count} records successfully",
                    records_processed=initial_count,
                    records_success=final_count,
                    records_error=0
                )

            return analytics_df

        except Exception as e:
            self.logger.log_error(
                ETLStep.TRANSFORM,
                f"Transformation failed: {str(e)}",
                exception=e
            )
            raise