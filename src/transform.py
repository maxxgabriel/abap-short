"""
ETL Transformer Module
Transforms raw sales data into analytics format
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import col, when, lit, current_timestamp, concat, date_format
import logging
from typing import Tuple


class ETLTransformer:
    """Transforms raw sales data into analytics format"""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger, config: dict, etl_run_id: str):
        """
        Initialize the transformer
        
        Args:
            spark: SparkSession instance
            logger: Logger instance for logging
            config: Configuration dictionary
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.etl_run_id = etl_run_id
        self._schema = self._get_analytics_schema()
    
    def _get_analytics_schema(self) -> StructType:
        """Define schema for analytics data"""
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
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[bool, DataFrame]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (success_flag, transformed_dataframe)
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Get configuration values
            discount_tier1_qty = self.config['business_rules']['discount_qty_tier1']
            discount_tier2_qty = self.config['business_rules']['discount_qty_tier2']
            discount_rate_tier1 = self.config['business_rules']['discount_rate_tier1']
            discount_rate_tier2 = self.config['business_rules']['discount_rate_tier2']
            tax_rate = self.config['business_rules']['tax_rate']
            cost_ratio = self.config['business_rules']['cost_ratio']
            category_high = self.config['business_rules']['category_high_threshold']
            category_medium = self.config['business_rules']['category_medium_threshold']
            
            # Calculate gross amount
            df_with_gross = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            
            # Calculate discount
            df_with_discount = df_with_gross.withColumn(
                "discount_amount",
                when(col("quantity") > discount_tier2_qty, 
                     col("gross_amount") * lit(discount_rate_tier2))
                .when(col("quantity") > discount_tier1_qty,
                      col("gross_amount") * lit(discount_rate_tier1))
                .otherwise(lit(0.0))
            )
            
            # Calculate tax
            df_with_tax = df_with_discount.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * lit(tax_rate)
            )
            
            # Calculate net amount
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Calculate profit margin
            df_with_profit = df_with_net.withColumn(
                "cost_amount",
                col("quantity") * col("unit_price") * lit(cost_ratio)
            ).withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     ((col("net_amount") - col("cost_amount")) / col("net_amount")) * lit(100.0))
                .otherwise(lit(0.0))
            )
            
            # Categorize sales
            df_with_category = df_with_profit.withColumn(
                "category",
                when(col("gross_amount") >= category_high, lit("HIGH"))
                .when(col("gross_amount") >= category_medium, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Generate analytics ID
            df_with_id = df_with_category.withColumn(
                "analytics_id",
                concat(
                    lit("ANL"),
                    col("trans_id"),
                    date_format(current_timestamp(), "HHmmss")
                )
            )
            
            # Add ETL metadata
            df_final = df_with_id.withColumn(
                "etl_run_id", lit(self.etl_run_id)
            ).withColumn(
                "loaded_at", current_timestamp()
            ).withColumn(
                "total_quantity", col("quantity")
            )
            
            # Select final columns
            analytics_df = df_final.select(
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
            
            record_count = analytics_df.count()
            
            self.logger.info(
                f"Transformed {record_count} records successfully",
                extra={
                    'step': 'TRANSFORM',
                    'status': 'S',
                    'records_processed': record_count,
                    'records_success': record_count
                }
            )
            
            return True, analytics_df
            
        except Exception as e:
            self.logger.error(
                f"Transformation failed: {str(e)}",
                extra={'step': 'TRANSFORM', 'status': 'E'},
                exc_info=True
            )
            return False, None