"""
Transform module for Sales ETL process.
Transforms raw sales data into analytics format with business rules.
"""

import logging
from typing import Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, 
    expr, round as spark_round, udf
)
from pyspark.sql.types import StringType

from src.logger import ETLLogger
from src.schemas import AnalyticsSchema


logger = logging.getLogger(__name__)


class Transformer:
    """Handles data transformation with business rules."""
    
    def __init__(self, spark: SparkSession, etl_logger: ETLLogger, config: dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            etl_logger: ETL logging instance
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.etl_logger = etl_logger
        self.config = config
        self.business_rules = config.get("business_rules", {})
        
    def transform_data(self, df_raw: DataFrame, etl_run_id: str) -> Tuple[bool, DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: Current ETL run identifier
            
        Returns:
            Tuple of (success flag, transformed DataFrame)
        """
        step = "TRANSFORM"
        
        try:
            self.etl_logger.log_message(
                step=step,
                status="S",
                message="Starting data transformation"
            )
            
            input_count = df_raw.count()
            
            # Apply transformations
            df_transformed = self._apply_business_rules(df_raw, etl_run_id)
            
            # Validate transformed data
            df_transformed = self._validate_transformations(df_transformed)
            
            output_count = df_transformed.count()
            error_count = input_count - output_count
            
            self.etl_logger.log_message(
                step=step,
                status="S",
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            logger.info(f"Transformation complete: {output_count} records")
            
            return True, df_transformed
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.etl_logger.log_message(
                step=step,
                status="E",
                message=error_msg
            )
            logger.error(error_msg, exc_info=True)
            return False, self.spark.createDataFrame([], AnalyticsSchema.get_schema())
            
    def _apply_business_rules(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Apply business rules to transform data.
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame
        """
        # Get business rule parameters from config
        discount_tier1_qty = self.business_rules.get("discount_qty_tier1", 10)
        discount_tier2_qty = self.business_rules.get("discount_qty_tier2", 15)
        discount_rate_tier1 = self.business_rules.get("discount_rate_tier1", 0.05)
        discount_rate_tier2 = self.business_rules.get("discount_rate_tier2", 0.10)
        tax_rate = self.business_rules.get("tax_rate", 0.08)
        cost_ratio = self.business_rules.get("cost_ratio", 0.60)
        category_high_threshold = self.business_rules.get("category_high_threshold", 2000.00)
        category_medium_threshold = self.business_rules.get("category_medium_threshold", 500.00)
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                expr("date_format(current_timestamp(), 'HHmmss')")
            )
        )
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_tier2_qty, 
                 col("gross_amount") * lit(discount_rate_tier2))
            .when(col("quantity") > discount_tier1_qty,
                  col("gross_amount") * lit(discount_rate_tier1))
            .otherwise(lit(0.0))
        )
        df = df.withColumn("discount_amount", spark_round(col("discount_amount"), 2))
        
        # Calculate tax amount (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * lit(tax_rate), 2)
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
        # Cost = quantity * unit_price * cost_ratio
        df = df.withColumn(
            "cost_amount",
            col("quantity") * col("unit_price") * lit(cost_ratio)
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                     2
                 ))
            .otherwise(lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= category_high_threshold, lit("HIGH"))
            .when(col("gross_amount") >= category_medium_threshold, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(etl_run_id))
        df = df.withColumn("loaded_at", current_timestamp())
        df = df.withColumn("loaded_by", lit("ETL_SYSTEM"))
        
        # Rename quantity column
        df = df.withColumn("total_quantity", col("quantity"))
        
        # Select final columns in order
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
        
    def _validate_transformations(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed records and filter out invalid ones.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Validated DataFrame
        """
        # Filter out records with invalid data
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        invalid_count = df.count() - df_valid.count()
        
        if invalid_count > 0:
            self.etl_logger.log_message(
                step="TRANSFORM",
                status="W",
                records_error=invalid_count,
                message=f"Filtered out {invalid_count} invalid records"
            )
            logger.warning(f"Filtered {invalid_count} invalid records")
            
        return df_valid
        
    def apply_data_quality_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply additional data quality rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with quality rules applied
        """
        # Trim string fields
        string_columns = ["customer_id", "product_id", "sales_rep", "region", "currency"]
        for col_name in string_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, expr(f"trim({col_name})"))
                
        # Uppercase currency codes
        if "currency" in df.columns:
            df = df.withColumn("currency", expr("upper(currency)"))
            
        # Ensure positive amounts
        numeric_columns = ["gross_amount", "net_amount", "discount_amount", "tax_amount"]
        for col_name in numeric_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(col(col_name) < 0, lit(0.0)).otherwise(col(col_name))
                )
                
        return df