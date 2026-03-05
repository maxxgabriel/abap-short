"""
Data Transformation Module
Transforms raw sales data into analytics format (maps to ZCL_ETL_TRANSFORMER).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, date_format, current_timestamp,
    round as spark_round, udf
)
from pyspark.sql.types import StringType, DecimalType
from typing import Dict
import logging
from datetime import datetime


class SalesDataTransformer:
    """
    Transforms raw sales data into analytics format.
    Maps to ABAP ZCL_ETL_TRANSFORMER class.
    """
    
    def __init__(self, spark: SparkSession, config: Dict, etl_run_id: str):
        """
        Initialize transformer with configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            etl_run_id: ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(__name__)
        
        # Load business rules from config
        self.discount_tier1_qty = config['transformer']['discount_qty_tier1']
        self.discount_tier2_qty = config['transformer']['discount_qty_tier2']
        self.discount_tier1_rate = config['transformer']['discount_rate_tier1']
        self.discount_tier2_rate = config['transformer']['discount_rate_tier2']
        self.tax_rate = config['transformer']['tax_rate']
        self.cost_ratio = config['transformer']['cost_ratio']
        self.category_high_threshold = config['transformer']['category_high_threshold']
        self.category_medium_threshold = config['transformer']['category_medium_threshold']
    
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data to analytics format.
        Maps to ABAP transform_data method.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Calculate analytics using business rules
            transformed_df = self._calculate_analytics(raw_df)
            
            record_count = transformed_df.count()
            self.logger.info(
                f"Transformed {record_count} records successfully"
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise TransformationException(f"Failed to transform data: {str(e)}")
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Calculate all analytics fields.
        Maps to ABAP calculate_analytics method.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            DataFrame with calculated analytics fields
        """
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > self.discount_tier2_qty,
                 spark_round(col("gross_amount") * self.discount_tier2_rate, 2))
            .when(col("quantity") > self.discount_tier1_qty,
                  spark_round(col("gross_amount") * self.discount_tier1_rate, 2))
            .otherwise(lit(0.0))
        )
        
        # Calculate tax on (gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * self.tax_rate, 2)
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
            spark_round(col("quantity") * col("unit_price") * self.cost_ratio, 2)
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
            self._categorize_sale_udf(col("gross_amount"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit(self.config['transformer']['analytics_id_prefix']),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.etl_run_id))
        df = df.withColumn("total_quantity", col("quantity"))
        
        # Select and rename columns to match analytics schema
        analytics_df = df.select(
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
            "etl_run_id"
        )
        
        return analytics_df
    
    @property
    def _categorize_sale_udf(self):
        """
        UDF for sale categorization.
        Maps to ABAP categorize_sale method.
        """
        def categorize(gross_amount):
            if gross_amount >= self.category_high_threshold:
                return self.config['transformer']['category_high']
            elif gross_amount >= self.category_medium_threshold:
                return self.config['transformer']['category_medium']
            else:
                return self.config['transformer']['category_low']
        
        return udf(categorize, StringType())


class TransformationException(Exception):
    """Custom exception for transformation errors."""
    pass