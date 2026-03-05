"""
Data transformation module for Sales ETL system.
Transforms raw sales data into analytics format with business logic.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
from typing import Tuple, Dict

from src.logger import ETLLogger
from src.constants import ETLConstants


class DataTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, logger: ETLLogger, config: Dict):
        """
        Initialize the data transformer.
        
        Args:
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
        self._analytics_schema = self._get_analytics_schema()
    
    def _get_analytics_schema(self) -> StructType:
        """Define schema for analytics data."""
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), True),
            StructField("loaded_by", StringType(), True)
        ])
    
    def transform_data(
        self, 
        raw_df: DataFrame,
        etl_run_id: str
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Tuple of (transformed DataFrame, success flag)
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._apply_transformations(raw_df, etl_run_id)
            
            # Validate transformed data
            final_count = transformed_df.count()
            error_count = initial_count - final_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            return raw_df.limit(0), False
    
    def _apply_transformations(
        self, 
        df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """Apply business logic transformations."""
        
        # Get configuration values
        discount_tier1_qty = self.config['business_rules']['discount_qty_tier1']
        discount_tier2_qty = self.config['business_rules']['discount_qty_tier2']
        discount_rate_tier1 = self.config['business_rules']['discount_rate_tier1']
        discount_rate_tier2 = self.config['business_rules']['discount_rate_tier2']
        tax_rate = self.config['business_rules']['tax_rate']
        cost_ratio = self.config['business_rules']['cost_ratio']
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(F.col("quantity") > discount_tier2_qty,
                   F.col("gross_amount") * F.lit(discount_rate_tier2))
            .when(F.col("quantity") > discount_tier1_qty,
                  F.col("gross_amount") * F.lit(discount_rate_tier1))
            .otherwise(F.lit(0.0))
        )
        
        # Calculate tax on discounted amount
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * F.lit(tax_rate)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(cost_ratio)
        )
        df = df.withColumn(
            "profit_margin",
            F.when(F.col("net_amount") > 0,
                   ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
            .otherwise(F.lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            self._categorize_sale_udf(F.col("gross_amount"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", F.lit(etl_run_id))
        df = df.withColumn("loaded_at", F.current_timestamp())
        df = df.withColumn("loaded_by", F.lit("ETL_SYSTEM"))
        
        # Rename quantity column
        df = df.withColumnRenamed("quantity", "total_quantity")
        
        # Select final columns in correct order
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
    
    @staticmethod
    @F.udf(returnType=StringType())
    def _categorize_sale_udf(gross_amount):
        """Categorize sales based on gross amount."""
        if gross_amount is None:
            return ETLConstants.CATEGORY_LOW
        
        if gross_amount >= 2000.00:
            return ETLConstants.CATEGORY_HIGH
        elif gross_amount >= 500.00:
            return ETLConstants.CATEGORY_MEDIUM
        else:
            return ETLConstants.CATEGORY_LOW