"""
ETL Transformer Module
Migrated from ZCL_ETL_TRANSFORMER ABAP class
Transforms raw sales data into analytics format
"""
from typing import Tuple, Optional
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, udf, lit, when

from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLTransformer:
    """
    ETL Transformer class for data transformation
    Migrated from ABAP ZCL_ETL_TRANSFORMER
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
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize ETL Transformer
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark
    
    def transform_data(
        self,
        raw_data: DataFrame
    ) -> Tuple[bool, Optional[DataFrame]]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_data: DataFrame with raw sales data
            
        Returns:
            Tuple of (success flag, DataFrame with transformed data)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.STEPS.TRANSFORM,
                status=ETLConstants.STATUS.SUCCESS,
                message="Starting data transformation"
            )
            
            if raw_data is None or raw_data.count() == 0:
                self.logger.log_message(
                    step=ETLConstants.STEPS.TRANSFORM,
                    status=ETLConstants.STATUS.WARNING,
                    message="No data to transform"
                )
                return False, None
            
            # Apply transformations
            analytics_df = self._apply_transformations(raw_data)
            
            # Validate transformed data
            analytics_df = self._validate_records(analytics_df)
            
            total_count = raw_data.count()
            success_count = analytics_df.count()
            error_count = total_count - success_count
            
            self.logger.log_message(
                step=ETLConstants.STEPS.TRANSFORM,
                status=ETLConstants.STATUS.SUCCESS,
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f"Transformed {success_count} of {total_count} records"
            )
            
            return True, analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.STEPS.TRANSFORM,
                status=ETLConstants.STATUS.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return False, None
    
    def _apply_transformations(self, raw_data: DataFrame) -> DataFrame:
        """
        Apply business transformations to raw data
        
        Args:
            raw_data: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame
        """
        from pyspark.sql.functions import concat, current_timestamp, date_format
        
        # Calculate gross amount
        df = raw_data.withColumn(
            "gross_amount",
            col("quantity") * col("unit_price")
        )
        
        # Calculate discount based on quantity
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > ETLConstants.RULES.DISCOUNT_QTY_TIER2,
                 col("gross_amount") * lit(ETLConstants.RULES.DISCOUNT_RATE_TIER2))
            .when(col("quantity") > ETLConstants.RULES.DISCOUNT_QTY_TIER1,
                  col("gross_amount") * lit(ETLConstants.RULES.DISCOUNT_RATE_TIER1))
            .otherwise(lit(0.0))
        )
        
        # Calculate tax
        df = df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * lit(ETLConstants.RULES.TAX_RATE)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            col("gross_amount") - col("discount_amount") + col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            col("quantity") * col("unit_price") * lit(ETLConstants.RULES.COST_RATIO)
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 ((col("net_amount") - col("cost_amount")) / col("net_amount") * 100))
            .otherwise(lit(0.0))
        )
        
        # Categorize sale
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= ETLConstants.RULES.CATEGORY_HIGH_THRESHOLD,
                 lit(ETLConstants.CATEGORIES.HIGH))
            .when(col("gross_amount") >= ETLConstants.RULES.CATEGORY_MEDIUM_THRESHOLD,
                  lit(ETLConstants.CATEGORIES.MEDIUM))
            .otherwise(lit(ETLConstants.CATEGORIES.LOW))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit(ETLConstants.PREFIXES.ANALYTICS_ID),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmssSSS")
            )
        )
        
        # Add ETL run ID
        df = df.withColumn(
            "etl_run_id",
            lit(self.logger.get_etl_run_id())
        )
        
        # Select and rename columns to match analytics schema
        analytics_df = df.select(
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
            col("category"),
            col("etl_run_id")
        )
        
        return analytics_df
    
    def _validate_records(self, analytics_df: DataFrame) -> DataFrame:
        """
        Validate transformed records
        
        Args:
            analytics_df: Analytics DataFrame
            
        Returns:
            Validated DataFrame with invalid records filtered out
        """
        # Filter out invalid records
        valid_df = analytics_df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(
                ETLConstants.CATEGORIES.HIGH,
                ETLConstants.CATEGORIES.MEDIUM,
                ETLConstants.CATEGORIES.LOW
            ))
        )
        
        return valid_df