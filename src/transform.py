"""
ETL Transformer Module
Transforms raw sales data into analytics format with business rules.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, round as spark_round, concat, current_timestamp, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
)
import logging


class ETLTransformer:
    """
    Transforms raw sales data into analytics format with business rules:
    - Calculate gross_amount (quantity * unit_price)
    - Apply tiered discount logic (10% for >15, 5% for >10, 0% default)
    - Calculate 8% tax on discounted amounts
    - Compute profit margin and categorize sales
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], logger):
        """
        Initialize transformer with Spark session and configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance for tracking transformations
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
        # Business rules from config
        self.discount_qty_tier1 = config['business_rules']['discount_qty_tier1']
        self.discount_qty_tier2 = config['business_rules']['discount_qty_tier2']
        self.discount_rate_tier1 = Decimal(str(config['business_rules']['discount_rate_tier1']))
        self.discount_rate_tier2 = Decimal(str(config['business_rules']['discount_rate_tier2']))
        self.tax_rate = Decimal(str(config['business_rules']['tax_rate']))
        self.cost_ratio = Decimal(str(config['business_rules']['cost_ratio']))
        self.category_high_threshold = Decimal(str(config['business_rules']['category_high_threshold']))
        self.category_medium_threshold = Decimal(str(config['business_rules']['category_medium_threshold']))
        
        self.etl_run_id = None
        
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics output data.
        
        Returns:
            StructType: Schema definition for analytics DataFrame
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def transform_data(self, df_raw: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: Unique ETL run identifier
            
        Returns:
            DataFrame: Transformed analytics DataFrame
        """
        self.etl_run_id = etl_run_id
        
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            initial_count = df_raw.count()
            
            # Step 1: Calculate gross amount
            df_with_gross = df_raw.withColumn(
                "gross_amount",
                spark_round(col("quantity") * col("unit_price"), 2)
            )
            
            # Step 2: Apply tiered discount logic
            df_with_discount = df_with_gross.withColumn(
                "discount_amount",
                spark_round(
                    when(col("quantity") > self.discount_qty_tier2, 
                         col("gross_amount") * lit(float(self.discount_rate_tier2)))
                    .when(col("quantity") > self.discount_qty_tier1,
                          col("gross_amount") * lit(float(self.discount_rate_tier1)))
                    .otherwise(lit(0.0)),
                    2
                )
            )
            
            # Step 3: Calculate discounted amount
            df_with_discounted = df_with_discount.withColumn(
                "discounted_amount",
                spark_round(col("gross_amount") - col("discount_amount"), 2)
            )
            
            # Step 4: Calculate tax (8% on discounted amount)
            df_with_tax = df_with_discounted.withColumn(
                "tax_amount",
                spark_round(col("discounted_amount") * lit(float(self.tax_rate)), 2)
            )
            
            # Step 5: Calculate net amount (discounted + tax)
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                spark_round(col("discounted_amount") + col("tax_amount"), 2)
            )
            
            # Step 6: Calculate profit margin
            df_with_profit = df_with_net.withColumn(
                "cost_amount",
                spark_round(col("quantity") * col("unit_price") * lit(float(self.cost_ratio)), 2)
            ).withColumn(
                "profit_margin",
                spark_round(
                    when(col("net_amount") > 0,
                         ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100)
                    .otherwise(lit(0.0)),
                    2
                )
            )
            
            # Step 7: Categorize sales
            df_with_category = df_with_profit.withColumn(
                "category",
                when(col("gross_amount") >= float(self.category_high_threshold), lit("HIGH"))
                .when(col("gross_amount") >= float(self.category_medium_threshold), lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Step 8: Generate analytics ID and add metadata
            df_analytics = df_with_category.withColumn(
                "analytics_id",
                concat(lit("ANL"), col("trans_id"), lit("_"), 
                       col("trans_date").cast("string"))
            ).withColumn(
                "total_quantity", col("quantity")
            ).withColumn(
                "etl_run_id", lit(etl_run_id)
            ).withColumn(
                "loaded_at", current_timestamp()
            )
            
            # Step 9: Select final columns
            df_final = df_analytics.select(
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
                "loaded_at"
            )
            
            final_count = df_final.count()
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=initial_count,
                records_success=final_count,
                records_error=initial_count - final_count,
                message=f'Transformed {final_count} of {initial_count} records'
            )
            
            return df_final
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            raise
    
    def validate_analytics_data(self, df_analytics: DataFrame) -> DataFrame:
        """
        Validate transformed analytics data.
        
        Args:
            df_analytics: Analytics DataFrame to validate
            
        Returns:
            DataFrame: Validated analytics DataFrame
        """
        try:
            # Remove records with invalid data
            df_valid = df_analytics.filter(
                (col("analytics_id").isNotNull()) &
                (col("customer_id").isNotNull()) &
                (col("product_id").isNotNull()) &
                (col("gross_amount") > 0) &
                (col("currency").isNotNull()) &
                (col("category").isin(["HIGH", "MEDIUM", "LOW"]))
            )
            
            initial_count = df_analytics.count()
            valid_count = df_valid.count()
            invalid_count = initial_count - valid_count
            
            if invalid_count > 0:
                self.logger.log_message(
                    step='VALIDATE',
                    status='W',
                    records_processed=initial_count,
                    records_success=valid_count,
                    records_error=invalid_count,
                    message=f'Removed {invalid_count} invalid records'
                )
            
            return df_valid
            
        except Exception as e:
            self.logger.log_message(
                step='VALIDATE',
                status='E',
                message=f'Validation failed: {str(e)}'
            )
            raise