"""
Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType,
    DecimalType, TimestampType
)
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ETLTransformationError


class DataTransformer:
    """
    Transforms raw sales data into analytics format.
    Applies business rules for discounts, taxes, and categorization.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the data transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance for logging
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.business_rules = config.get("business_rules", {})
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        Maps ABAP analytics table structure to PySpark StructType.
        
        Returns:
            StructType: Schema definition for analytics data
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
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
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
            Tuple of (transformed DataFrame, success boolean)
            
        Raises:
            ETLTransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="I",
                message="Starting data transformation"
            )
            
            initial_count = df_raw.count()
            
            # Apply transformations
            df_analytics = self._calculate_analytics(df_raw, etl_run_id)
            
            # Validate transformed data
            df_validated = self._validate_transformed_records(df_analytics)
            
            final_count = df_validated.count()
            error_count = initial_count - final_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return df_validated, True
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=error_msg
            )
            raise ETLTransformationError(error_msg, step="TRANSFORM") from e
    
    def _calculate_analytics(self, df_raw: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Calculate analytics metrics with business rules.
        
        Business Rules (from ABAP code):
        - Discount: 5% for quantity > 10, 10% for quantity > 15
        - Tax: 8% on (gross - discount)
        - Cost ratio: 60% of unit price
        - Category: HIGH (>=2000), MEDIUM (>=500), LOW (<500)
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame with analytics metrics
        """
        # Get business rules from config
        discount_tier1_qty = self.business_rules.get("discount_qty_tier1", 10)
        discount_tier2_qty = self.business_rules.get("discount_qty_tier2", 15)
        discount_tier1_rate = self.business_rules.get("discount_rate_tier1", 0.05)
        discount_tier2_rate = self.business_rules.get("discount_rate_tier2", 0.10)
        tax_rate = self.business_rules.get("tax_rate", 0.08)
        cost_ratio = self.business_rules.get("cost_ratio", 0.60)
        category_high_threshold = self.business_rules.get("category_high_threshold", 2000.00)
        category_medium_threshold = self.business_rules.get("category_medium_threshold", 500.00)
        
        # Calculate gross amount
        df_with_gross = df_raw.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df_with_discount = df_with_gross.withColumn(
            "discount_amount",
            spark_round(
                when(col("quantity") > discount_tier2_qty, 
                     col("gross_amount") * lit(discount_tier2_rate))
                .when(col("quantity") > discount_tier1_qty, 
                      col("gross_amount") * lit(discount_tier1_rate))
                .otherwise(lit(0.0)),
                2
            )
        )
        
        # Calculate tax (8% on gross - discount)
        df_with_tax = df_with_discount.withColumn(
            "tax_amount",
            spark_round(
                (col("gross_amount") - col("discount_amount")) * lit(tax_rate),
                2
            )
        )
        
        # Calculate net amount
        df_with_net = df_with_tax.withColumn(
            "net_amount",
            spark_round(
                col("gross_amount") - col("discount_amount") + col("tax_amount"),
                2
            )
        )
        
        # Calculate profit margin
        # Cost = quantity * unit_price * cost_ratio
        # Profit margin = ((net - cost) / net) * 100
        df_with_profit = df_with_net.withColumn(
            "cost_amount",
            col("quantity") * col("unit_price") * lit(cost_ratio)
        ).withColumn(
            "profit_margin",
            spark_round(
                when(col("net_amount") > 0,
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100))
                .otherwise(lit(0.0)),
                2
            )
        )
        
        # Categorize sales
        df_with_category = df_with_profit.withColumn(
            "category",
            when(col("gross_amount") >= lit(category_high_threshold), lit("HIGH"))
            .when(col("gross_amount") >= lit(category_medium_threshold), lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Generate analytics ID (ANL + trans_id + timestamp suffix)
        df_with_id = df_with_category.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                lit("_"),
                current_timestamp().cast("long")
            )
        )
        
        # Add metadata columns
        df_final = df_with_id.withColumn(
            "etl_run_id", lit(etl_run_id)
        ).withColumn(
            "loaded_at", current_timestamp()
        ).withColumn(
            "loaded_by", lit("pyspark_etl")
        ).withColumn(
            "total_quantity", col("quantity")
        )
        
        # Select final columns in proper order
        df_analytics = df_final.select(
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
        
        return df_analytics
    
    def _validate_transformed_records(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed records and filter out invalid ones.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        # Filter out records with invalid values
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        return df_valid
    
    def categorize_sale(self, gross_amount: float) -> str:
        """
        Categorize sale based on gross amount.
        Helper function for testing and debugging.
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Category string: HIGH, MEDIUM, or LOW
        """
        thresholds = self.business_rules
        high_threshold = thresholds.get("category_high_threshold", 2000.00)
        medium_threshold = thresholds.get("category_medium_threshold", 500.00)
        
        if gross_amount >= high_threshold:
            return "HIGH"
        elif gross_amount >= medium_threshold:
            return "MEDIUM"
        else:
            return "LOW"