"""
Data Transformation Module for Sales ETL System

Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.constants import ETLConstants
from src.exceptions import ETLTransformationError


class SalesTransformer:
    """Transforms raw sales data into analytics format."""
    
    # Schema for analytics output
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
        StructField("etl_run_id", StringType(), False),
        StructField("loaded_at", TimestampType(), False),
        StructField("loaded_by", StringType(), True)
    ])
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.constants = ETLConstants()
        self.log = logging.getLogger(__name__)
    
    def transform_data(
        self, 
        df_raw: DataFrame,
        etl_run_id: str
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: Unique ETL run identifier
            
        Returns:
            Tuple of (DataFrame, success_flag)
            
        Raises:
            ETLTransformationError: If transformation fails
        """
        try:
            step = "TRANSFORM"
            self.logger.log_message(
                step=step,
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = df_raw.count()
            
            # Apply transformations
            df_transformed = self._apply_business_rules(df_raw, etl_run_id)
            
            # Validate transformed data
            df_valid, df_invalid = self._validate_transformed_data(df_transformed)
            
            valid_count = df_valid.count()
            invalid_count = df_invalid.count()
            
            # Log invalid records
            if invalid_count > 0:
                self._log_invalid_records(df_invalid)
            
            # Log transformation results
            self.logger.log_message(
                step=step,
                status="S",
                records_processed=initial_count,
                records_success=valid_count,
                records_error=invalid_count,
                message=f"Transformed {valid_count} of {initial_count} records"
            )
            
            return df_valid, True
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=error_msg
            )
            raise ETLTransformationError(error_msg, step="TRANSFORM") from e
    
    def _apply_business_rules(
        self, 
        df_raw: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Apply business rules and calculate derived fields.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame
        """
        # Calculate gross amount
        df = df_raw.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.constants.DISCOUNT_QTY_TIER2,
                F.col("gross_amount") * F.lit(self.constants.DISCOUNT_RATE_TIER2)
            ).when(
                F.col("quantity") > self.constants.DISCOUNT_QTY_TIER1,
                F.col("gross_amount") * F.lit(self.constants.DISCOUNT_RATE_TIER1)
            ).otherwise(F.lit(0.0))
        )
        
        # Calculate tax amount
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * 
            F.lit(self.constants.TAX_RATE)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(self.constants.COST_RATIO)
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= self.constants.CATEGORY_HIGH_THRESHOLD,
                F.lit(self.constants.CATEGORY_HIGH)
            ).when(
                F.col("gross_amount") >= self.constants.CATEGORY_MEDIUM_THRESHOLD,
                F.lit(self.constants.CATEGORY_MEDIUM)
            ).otherwise(F.lit(self.constants.CATEGORY_LOW))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit(self.constants.PREFIX_ANALYTICS_ID),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add metadata fields
        df = df.withColumn("etl_run_id", F.lit(etl_run_id))
        df = df.withColumn("loaded_at", F.current_timestamp())
        df = df.withColumn("loaded_by", F.lit("ETL_SYSTEM"))
        
        # Rename quantity field
        df = df.withColumnRenamed("quantity", "total_quantity")
        
        # Select final columns in correct order
        df = df.select(
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
        
        return df
    
    def _validate_transformed_data(
        self, 
        df: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Validate transformed records and split into valid/invalid.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        # Define validation conditions
        validation_expr = (
            F.col("analytics_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("product_id").isNotNull() &
            (F.col("gross_amount") > 0) &
            F.col("currency").isNotNull() &
            F.col("category").isin([
                self.constants.CATEGORY_HIGH,
                self.constants.CATEGORY_MEDIUM,
                self.constants.CATEGORY_LOW
            ])
        )
        
        # Split into valid and invalid
        df_valid = df.filter(validation_expr)
        df_invalid = df.filter(~validation_expr)
        
        return df_valid, df_invalid
    
    def _log_invalid_records(self, df_invalid: DataFrame) -> None:
        """
        Log details of invalid records for troubleshooting.
        
        Args:
            df_invalid: DataFrame with invalid records
        """
        invalid_count = df_invalid.count()
        if invalid_count > 0:
            self.log.warning(f"Found {invalid_count} invalid records")
            
            # Sample invalid records for logging
            sample_size = min(10, invalid_count)
            invalid_samples = df_invalid.limit(sample_size).collect()
            
            for row in invalid_samples:
                self.log.warning(f"Invalid record: {row.asDict()}")