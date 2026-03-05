"""
Data transformation module for Sales ETL pipeline.
Applies business logic including discount tiers, tax calculation, and categorization.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, 
    concat, current_timestamp, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
import logging

logger = logging.getLogger(__name__)


class SalesTransformer:
    """Transforms raw sales data into analytics format with business rules."""
    
    def __init__(self, config: dict):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Configuration dictionary containing business rules
        """
        self.config = config
        self.logger = logger
        
        # Load business rules from config
        self.discount_tier1_qty = config['business_rules']['discount_qty_tier1']
        self.discount_tier2_qty = config['business_rules']['discount_qty_tier2']
        self.discount_rate_tier1 = config['business_rules']['discount_rate_tier1']
        self.discount_rate_tier2 = config['business_rules']['discount_rate_tier2']
        self.tax_rate = config['business_rules']['tax_rate']
        self.cost_ratio = config['business_rules']['cost_ratio']
        self.category_high_threshold = config['business_rules']['category_high_threshold']
        self.category_medium_threshold = config['business_rules']['category_medium_threshold']
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        
        Returns:
            StructType schema matching ZSALES_ANALYTICS table structure
        """
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
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data applying all business rules.
        
        Args:
            raw_df: DataFrame containing raw sales data
            etl_run_id: Unique identifier for this ETL run
            
        Returns:
            DataFrame containing transformed analytics data
        """
        self.logger.info("Starting data transformation with business rules")
        
        try:
            # Step 1: Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Step 2: Calculate discount based on quantity tiers
            df = df.withColumn(
                "discount_amount",
                when(col("quantity") > self.discount_tier2_qty, 
                     spark_round(col("gross_amount") * lit(self.discount_rate_tier2), 2))
                .when(col("quantity") > self.discount_tier1_qty,
                      spark_round(col("gross_amount") * lit(self.discount_rate_tier1), 2))
                .otherwise(lit(0.0))
            )
            
            # Step 3: Calculate tax (8% on gross minus discount)
            df = df.withColumn(
                "tax_amount",
                spark_round((col("gross_amount") - col("discount_amount")) * lit(self.tax_rate), 2)
            )
            
            # Step 4: Calculate net amount (gross - discount + tax)
            df = df.withColumn(
                "net_amount",
                spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
            )
            
            # Step 5: Calculate cost and profit margin
            df = df.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(self.cost_ratio), 2)
            )
            
            df = df.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100), 2))
                .otherwise(lit(0.0))
            )
            
            # Step 6: Categorize sales based on gross amount
            df = df.withColumn(
                "category",
                when(col("gross_amount") >= self.category_high_threshold, lit("HIGH"))
                .when(col("gross_amount") >= self.category_medium_threshold, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Step 7: Generate analytics ID
            df = df.withColumn(
                "analytics_id",
                concat(
                    lit("ANL"),
                    col("trans_id"),
                    monotonically_increasing_id().cast(StringType())
                )
            )
            
            # Step 8: Add ETL metadata
            df = df.withColumn("etl_run_id", lit(etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            
            # Step 9: Select and rename columns for final output
            analytics_df = df.select(
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
            
            record_count = analytics_df.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            # Log transformation statistics
            self._log_transformation_stats(analytics_df)
            
            return analytics_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _log_transformation_stats(self, df: DataFrame) -> None:
        """
        Log statistics about transformed data.
        
        Args:
            df: Transformed analytics DataFrame
        """
        try:
            # Count by category
            category_counts = df.groupBy("category").count().collect()
            
            for row in category_counts:
                self.logger.info(f"Category {row['category']}: {row['count']} records")
            
            # Calculate aggregate metrics
            totals = df.agg({
                "gross_amount": "sum",
                "discount_amount": "sum",
                "tax_amount": "sum",
                "net_amount": "sum"
            }).collect()[0]
            
            self.logger.info(f"Total gross amount: {totals['sum(gross_amount)']}")
            self.logger.info(f"Total discount: {totals['sum(discount_amount)']}")
            self.logger.info(f"Total tax: {totals['sum(tax_amount)']}")
            self.logger.info(f"Total net amount: {totals['sum(net_amount)']}")
            
        except Exception as e:
            self.logger.warning(f"Could not log transformation stats: {str(e)}")
    
    def validate_transformed_data(self, df: DataFrame) -> bool:
        """
        Validate transformed data meets quality requirements.
        
        Args:
            df: Transformed analytics DataFrame
            
        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info("Validating transformed data")
        
        try:
            # Check for null values in required fields
            required_fields = ["analytics_id", "customer_id", "product_id", "gross_amount"]
            
            for field in required_fields:
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    self.logger.error(f"Field {field} has {null_count} null values")
                    return False
            
            # Check for negative amounts
            negative_check = df.filter(
                (col("gross_amount") < 0) | 
                (col("net_amount") < 0) |
                (col("discount_amount") < 0) |
                (col("tax_amount") < 0)
            ).count()
            
            if negative_check > 0:
                self.logger.error(f"Found {negative_check} records with negative amounts")
                return False
            
            # Check valid categories
            invalid_categories = df.filter(
                ~col("category").isin(["HIGH", "MEDIUM", "LOW"])
            ).count()
            
            if invalid_categories > 0:
                self.logger.error(f"Found {invalid_categories} records with invalid categories")
                return False
            
            self.logger.info("Data validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False