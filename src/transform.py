"""
PySpark Data Transformer Module
Transforms raw sales data into analytics format with business logic.
"""

from decimal import Decimal
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, 
    round as spark_round, substring
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType
)
from src.logger import ETLLogger


class SalesDataTransformer:
    """
    Transforms raw sales data into analytics format.
    Applies business rules for discounts, taxes, and categorization.
    """
    
    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize transformer with logger and configuration.
        
        Args:
            logger: ETLLogger instance for tracking operations
            config: Configuration dictionary with business rules
        """
        self.logger = logger
        self.config = config
        self._schema = self._define_schema()
    
    def _define_schema(self) -> StructType:
        """
        Define schema for analytics data.
        
        Returns:
            StructType schema for transformed data
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
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame with transformed analytics data
            
        Raises:
            RuntimeError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Get business rules from config
            discount_tier1_qty = self.config['business_rules']['discount_tier1_quantity']
            discount_tier2_qty = self.config['business_rules']['discount_tier2_quantity']
            discount_tier1_rate = Decimal(str(self.config['business_rules']['discount_tier1_rate']))
            discount_tier2_rate = Decimal(str(self.config['business_rules']['discount_tier2_rate']))
            tax_rate = Decimal(str(self.config['business_rules']['tax_rate']))
            cost_ratio = Decimal(str(self.config['business_rules']['cost_ratio']))
            category_high = self.config['business_rules']['category_high_threshold']
            category_medium = self.config['business_rules']['category_medium_threshold']
            
            # Calculate gross amount
            transformed_df = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            transformed_df = transformed_df.withColumn(
                "discount_amount",
                when(col("quantity") > discount_tier2_qty, 
                     spark_round(col("gross_amount") * lit(float(discount_tier2_rate)), 2))
                .when(col("quantity") > discount_tier1_qty,
                      spark_round(col("gross_amount") * lit(float(discount_tier1_rate)), 2))
                .otherwise(lit(0.00))
            )
            
            # Calculate tax on discounted amount
            transformed_df = transformed_df.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(float(tax_rate)), 2)
            )
            
            # Calculate net amount
            transformed_df = transformed_df.withColumn(
                "net_amount",
                spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
            )
            
            # Calculate profit margin
            transformed_df = transformed_df.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(float(cost_ratio)), 2)
            )
            
            transformed_df = transformed_df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2))
                .otherwise(lit(0.00))
            )
            
            # Categorize sales
            transformed_df = transformed_df.withColumn(
                "category",
                when(col("gross_amount") >= category_high, lit("HIGH"))
                .when(col("gross_amount") >= category_medium, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            transformed_df = transformed_df.withColumn(
                "analytics_id",
                concat(
                    lit("ANL"),
                    col("trans_id"),
                    substring(current_timestamp().cast("string"), 12, 6)
                )
            )
            
            # Add ETL run ID
            transformed_df = transformed_df.withColumn(
                "etl_run_id",
                lit(self.logger.get_etl_run_id())
            )
            
            # Select final columns
            final_df = transformed_df.select(
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
            
            # Cache for performance
            final_df.cache()
            output_count = final_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return final_df
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=error_msg
            )
            raise RuntimeError(error_msg) from e
    
    def get_schema(self) -> StructType:
        """
        Get the schema used for transformation output.
        
        Returns:
            StructType schema definition
        """
        return self._schema