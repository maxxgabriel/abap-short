"""
Data Transformation Module
Transforms raw sales data into analytics format with business logic
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, 
    round as spark_round, concat, date_format
)
from typing import Optional
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import ETLTransformError


class SalesDataTransformer:
    """Transforms raw sales data with business rules"""
    
    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the transformer
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self._setup_schema()
        self._load_business_rules()
    
    def _setup_schema(self) -> None:
        """Define schema for analytics data"""
        self.analytics_schema = StructType([
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
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def _load_business_rules(self) -> None:
        """Load business rules from configuration"""
        rules = self.config['business_rules']
        
        self.tax_rate = rules['tax_rate']
        self.cost_ratio = rules['cost_ratio']
        self.discount_tier1_qty = rules['discount']['tier1_quantity']
        self.discount_tier1_rate = rules['discount']['tier1_rate']
        self.discount_tier2_qty = rules['discount']['tier2_quantity']
        self.discount_tier2_rate = rules['discount']['tier2_rate']
        self.category_high_threshold = rules['category']['high_threshold']
        self.category_medium_threshold = rules['category']['medium_threshold']
    
    def transform_data(
        self,
        raw_df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
            
        Raises:
            ETLTransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(col("quantity") > self.discount_tier2_qty,
                     spark_round(col("gross_amount") * lit(self.discount_tier2_rate), 2))
                .when(col("quantity") > self.discount_tier1_qty,
                      spark_round(col("gross_amount") * lit(self.discount_tier1_rate), 2))
                .otherwise(lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            df = df.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(self.tax_rate), 2)
            )
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
            )
            
            # Calculate profit margin
            df = df.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(self.cost_ratio), 2)
            )
            
            df = df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount") * 100), 2))
                .otherwise(lit(0.0))
            )
            
            # Categorize sales
            df = df.withColumn(
                "category",
                when(col("gross_amount") >= self.category_high_threshold, lit("HIGH"))
                .when(col("gross_amount") >= self.category_medium_threshold, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            df = df.withColumn(
                "analytics_id",
                concat(
                    lit("ANL"),
                    col("trans_id"),
                    date_format(current_timestamp(), "HHmmss")
                )
            )
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", lit(etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            
            # Select and rename columns for final schema
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
                col("etl_run_id"),
                col("loaded_at")
            )
            
            record_count = analytics_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Transformed {record_count} records successfully"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise ETLTransformError(f"Failed to transform data: {str(e)}") from e
    
    def validate_transformed_data(self, df: DataFrame) -> bool:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if df is None or df.count() == 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="W",
                message="No data to validate"
            )
            return False
        
        # Check for negative amounts
        negative_amounts = df.filter(
            (col("gross_amount") < 0) | 
            (col("net_amount") < 0)
        ).count()
        
        if negative_amounts > 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="W",
                message=f"Found {negative_amounts} records with negative amounts"
            )
        
        # Check for invalid categories
        invalid_categories = df.filter(
            ~col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()
        
        if invalid_categories > 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Found {invalid_categories} records with invalid categories"
            )
            return False
        
        return True