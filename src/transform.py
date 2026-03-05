"""
Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.config import Config
from src.exceptions import TransformError


class Transformer:
    """
    Handles transformation of raw sales data into analytics format.
    
    Attributes:
        config: Configuration object with business rules
        logger: ETL logger instance
    """
    
    def __init__(self, config: Config, logger: ETLLogger):
        """
        Initialize the transformer.
        
        Args:
            config: Configuration object
            logger: ETL logger instance
        """
        self.config = config
        self.logger = logger
    
    def transform_data(self, raw_data: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_data: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
            
        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = raw_data.count()
            
            # Apply transformations
            df = self._calculate_amounts(raw_data)
            df = self._categorize_sales(df)
            df = self._add_analytics_metadata(df, etl_run_id)
            df = self._select_final_columns(df)
            
            final_count = df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(
                error_text=f"Failed to transform data: {str(e)}",
                error_step="TRANSFORM"
            ) from e
    
    def _calculate_amounts(self, df: DataFrame) -> DataFrame:
        """
        Calculate financial amounts with business rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated amounts
        """
        # Calculate gross amount
        df = df.withColumn("gross_amount", col("quantity") * col("unit_price"))
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(
                col("quantity") > self.config.business_rules.discount_qty_tier2,
                col("gross_amount") * self.config.business_rules.discount_rate_tier2
            ).when(
                col("quantity") > self.config.business_rules.discount_qty_tier1,
                col("gross_amount") * self.config.business_rules.discount_rate_tier1
            ).otherwise(lit(0.0))
        )
        
        # Calculate tax (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * self.config.business_rules.tax_rate
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            col("gross_amount") - col("discount_amount") + col("tax_amount")
        )
        
        # Calculate profit margin
        # Cost = quantity * unit_price * cost_ratio
        df = df.withColumn(
            "cost_amount",
            col("quantity") * col("unit_price") * self.config.business_rules.cost_ratio
        )
        
        df = df.withColumn(
            "profit_margin",
            when(
                col("net_amount") > 0,
                ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100
            ).otherwise(lit(0.0))
        )
        
        return df
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount thresholds.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category column
        """
        df = df.withColumn(
            "category",
            when(
                col("gross_amount") >= self.config.business_rules.category_high_threshold,
                lit("HIGH")
            ).when(
                col("gross_amount") >= self.config.business_rules.category_medium_threshold,
                lit("MEDIUM")
            ).otherwise(lit("LOW"))
        )
        
        return df
    
    def _add_analytics_metadata(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add analytics metadata (IDs, timestamps).
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with metadata columns
        """
        # Generate analytics ID using UDF
        def generate_analytics_id():
            unique_id = str(uuid.uuid4()).replace("-", "")[:14].upper()
            return f"ANL{unique_id}"
        
        analytics_id_udf = udf(lambda: generate_analytics_id(), StringType())
        
        df = df.withColumn("analytics_id", analytics_id_udf())
        df = df.withColumn("etl_run_id", lit(etl_run_id))
        df = df.withColumn("loaded_at", lit(datetime.now()))
        
        return df
    
    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        """
        Select and order final columns for analytics table.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with final column selection
        """
        return df.select(
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
            "etl_run_id",
            "loaded_at"
        )
    
    def get_analytics_schema(self) -> StructType:
        """
        Get the schema for analytics data.
        
        Returns:
            StructType schema definition
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
            StructField("profit_margin", DecimalType(5, 2), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", StringType(), nullable=False)
        ])