"""
Data Transformation Module
Replaces ZCL_ETL_TRANSFORMER class
"""
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType
)

from src.utils.logger import ETLLogger
from src.utils.exceptions import ETLError


class DataTransformer:
    """
    Transforms raw sales data into analytics format
    Replaces ZCL_ETL_TRANSFORMER
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        logger: ETLLogger
    ):
        """Initialize transformer"""
        self.spark = spark
        self.config = config
        self.logger = logger
        self.business_rules = config['business_rules']
    
    def transform_data(self, raw_data: DataFrame) -> DataFrame:
        """
        Transform raw data to analytics format
        
        Args:
            raw_data: DataFrame with raw sales data
            
        Returns:
            DataFrame with analytics data
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            input_count = raw_data.count()
            
            # Apply transformations
            analytics_df = self._apply_transformations(raw_data)
            
            output_count = analytics_df.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f"Transformation failed: {str(e)}"
            )
            raise ETLError(f"Transformation failed: {str(e)}") from e
    
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply all business rule transformations"""
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit(self.config['id_prefixes']['analytics_id']),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), 'HHmmss')
            )
        )
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        discount_rules = self.business_rules['discount']
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > discount_rules['tier2_quantity'],
                F.col("gross_amount") * F.lit(discount_rules['tier2_rate'])
            ).when(
                F.col("quantity") > discount_rules['tier1_quantity'],
                F.col("gross_amount") * F.lit(discount_rules['tier1_rate'])
            ).otherwise(F.lit(0.0))
        )
        
        # Calculate tax (on gross - discount)
        tax_rate = self.business_rules['tax_rate']
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
        cost_ratio = self.business_rules['cost_ratio']
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(cost_ratio)
        )
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0.0))
        )
        
        # Categorize sales
        category_rules = self.business_rules['category']
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= category_rules['high_threshold'],
                F.lit(self.config['etl']['categories']['high'])
            ).when(
                F.col("gross_amount") >= category_rules['medium_threshold'],
                F.lit(self.config['etl']['categories']['medium'])
            ).otherwise(
                F.lit(self.config['etl']['categories']['low'])
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", F.lit(self.logger.etl_run_id))
        df = df.withColumn("loaded_at", F.current_timestamp())
        
        # Select and rename columns for final output
        analytics_df = df.select(
            "analytics_id",
            F.col("trans_date"),
            "customer_id",
            "product_id",
            F.col("quantity").alias("total_quantity"),
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
        
        return analytics_df
    
    @staticmethod
    def get_schema() -> StructType:
        """Get schema for analytics data"""
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
            StructField("loaded_at", StringType(), False)
        ])