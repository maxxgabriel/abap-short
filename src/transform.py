"""
PySpark Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round, concat, current_timestamp
from pyspark.sql.types import DecimalType
from typing import Dict
import logging


class DataTransformer:
    """Handles transformation of raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, config: Dict, logger: logging.Logger):
        """
        Initialize DataTransformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.transform_config = config.get('transform', {})
        self.business_rules = self.transform_config.get('business_rules', {})
        
    def transform_data(self, df: DataFrame, etl_run_id: str) -> Dict:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Dictionary containing transformed DataFrame and statistics
        """
        try:
            self.logger.info("Starting data transformation")
            
            total_records = df.count()
            self.logger.info(f"Total records to transform: {total_records}")
            
            # Apply transformations
            df_transformed = self._calculate_analytics(df, etl_run_id)
            
            success_count = df_transformed.count()
            
            self.logger.info(f"Transformed {success_count} of {total_records} records")
            
            return {
                'success': True,
                'dataframe': df_transformed,
                'total_records': total_records,
                'success_records': success_count,
                'error_records': total_records - success_count,
                'message': f'Transformed {success_count} of {total_records} records'
            }
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            return {
                'success': False,
                'dataframe': None,
                'total_records': 0,
                'success_records': 0,
                'error_records': 0,
                'message': f'Transformation failed: {str(e)}'
            }
    
    def _calculate_analytics(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Calculate analytics metrics with business rules.
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame with analytics
        """
        # Get business rule parameters
        discount_qty_tier1 = self.business_rules.get('discount_qty_tier1', 10)
        discount_qty_tier2 = self.business_rules.get('discount_qty_tier2', 15)
        discount_rate_tier1 = self.business_rules.get('discount_rate_tier1', 0.05)
        discount_rate_tier2 = self.business_rules.get('discount_rate_tier2', 0.10)
        tax_rate = self.business_rules.get('tax_rate', 0.08)
        cost_ratio = self.business_rules.get('cost_ratio', 0.60)
        category_high_threshold = self.business_rules.get('category_high_threshold', 2000.00)
        category_medium_threshold = self.business_rules.get('category_medium_threshold', 500.00)
        
        # Calculate gross amount
        df_with_gross = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df_with_discount = df_with_gross.withColumn(
            "discount_amount",
            when(col("quantity") > discount_qty_tier2, 
                 spark_round(col("gross_amount") * lit(discount_rate_tier2), 2))
            .when(col("quantity") > discount_qty_tier1,
                  spark_round(col("gross_amount") * lit(discount_rate_tier1), 2))
            .otherwise(lit(0.0))
        )
        
        # Calculate tax on (gross - discount)
        df_with_tax = df_with_discount.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * lit(tax_rate), 2)
        )
        
        # Calculate net amount
        df_with_net = df_with_tax.withColumn(
            "net_amount",
            spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
        )
        
        # Calculate profit margin
        df_with_profit = df_with_net.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * lit(cost_ratio), 2)
        ).withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2))
            .otherwise(lit(0.0))
        )
        
        # Categorize sales
        df_with_category = df_with_profit.withColumn(
            "category",
            when(col("gross_amount") >= lit(category_high_threshold), lit("HIGH"))
            .when(col("gross_amount") >= lit(category_medium_threshold), lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Generate analytics ID
        df_with_id = df_with_category.withColumn(
            "analytics_id",
            concat(lit("ANL"), col("trans_id"))
        )
        
        # Add ETL metadata
        df_final = df_with_id.withColumn("etl_run_id", lit(etl_run_id))
        
        # Select and rename columns for final output
        df_analytics = df_final.select(
            col("analytics_id"),
            col("trans_date"),
            col("customer_id"),
            col("product_id"),
            col("quantity").alias("total_quantity"),
            col("gross_amount").cast(DecimalType(16, 2)),
            col("net_amount").cast(DecimalType(16, 2)),
            col("discount_amount").cast(DecimalType(16, 2)),
            col("tax_amount").cast(DecimalType(16, 2)),
            col("currency"),
            col("sales_rep"),
            col("region"),
            col("profit_margin").cast(DecimalType(5, 2)),
            col("category"),
            col("etl_run_id")
        )
        
        return df_analytics
    
    def _categorize_sale(self, gross_amount: float) -> str:
        """
        Categorize sale based on gross amount.
        
        Args:
            gross_amount: Gross sale amount
            
        Returns:
            Category string (HIGH, MEDIUM, LOW)
        """
        category_high_threshold = self.business_rules.get('category_high_threshold', 2000.00)
        category_medium_threshold = self.business_rules.get('category_medium_threshold', 500.00)
        
        if gross_amount >= category_high_threshold:
            return "HIGH"
        elif gross_amount >= category_medium_threshold:
            return "MEDIUM"
        else:
            return "LOW"


def create_transformer(spark: SparkSession, config: Dict, logger: logging.Logger) -> DataTransformer:
    """
    Factory function to create DataTransformer instance.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        logger: Logger instance
        
    Returns:
        DataTransformer instance
    """
    return DataTransformer(spark, config, logger)