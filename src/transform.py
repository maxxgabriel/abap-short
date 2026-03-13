"""
Data transformation module for Sales ETL process.
Implements business logic for discount tiers and tax calculations.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, 
    concat, current_timestamp, date_format
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Tuple
import logging


class SalesDataTransformer:
    """Transforms raw sales data into analytics format with business rules applied."""
    
    def __init__(self, config: dict, logger: logging.Logger, etl_run_id: str):
        """
        Initialize transformer.
        
        Args:
            config: Configuration dictionary
            logger: Logger instance
            etl_run_id: Unique ETL run identifier
        """
        self.config = config
        self.logger = logger
        self.etl_run_id = etl_run_id
        self.business_rules = config.get('business_rules', {})
        self.analytics_schema = self._get_analytics_schema()
    
    def _get_analytics_schema(self) -> StructType:
        """Define schema for analytics output."""
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
            StructField("loaded_at", TimestampType(), False),
        ])
    
    def transform_sales_data(self, raw_df: DataFrame) -> Tuple[DataFrame, dict]:
        """
        Transform raw sales data applying business rules.
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            Tuple of (transformed DataFrame, transformation statistics)
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Get business rule parameters
            discount_tier1_qty = self.business_rules.get('discount_qty_tier1', 10)
            discount_tier2_qty = self.business_rules.get('discount_qty_tier2', 15)
            discount_tier1_rate = self.business_rules.get('discount_rate_tier1', 0.05)
            discount_tier2_rate = self.business_rules.get('discount_rate_tier2', 0.10)
            tax_rate = self.business_rules.get('tax_rate', 0.08)
            cost_ratio = self.business_rules.get('cost_ratio', 0.60)
            category_high = self.business_rules.get('category_high_threshold', 2000.00)
            category_medium = self.business_rules.get('category_medium_threshold', 500.00)
            
            # Calculate gross amount
            df = raw_df.withColumn(
                'gross_amount',
                spark_round(col('quantity') * col('unit_price'), 2)
            )
            
            # Apply discount tiers using when/otherwise
            df = df.withColumn(
                'discount_amount',
                when(col('quantity') > discount_tier2_qty, 
                     spark_round(col('gross_amount') * lit(discount_tier2_rate), 2))
                .when(col('quantity') > discount_tier1_qty,
                      spark_round(col('gross_amount') * lit(discount_tier1_rate), 2))
                .otherwise(lit(0.00))
            )
            
            # Calculate taxable amount
            df = df.withColumn(
                'taxable_amount',
                col('gross_amount') - col('discount_amount')
            )
            
            # Apply tax rate
            df = df.withColumn(
                'tax_amount',
                spark_round(col('taxable_amount') * lit(tax_rate), 2)
            )
            
            # Calculate net amount
            df = df.withColumn(
                'net_amount',
                spark_round(col('taxable_amount') + col('tax_amount'), 2)
            )
            
            # Calculate cost and profit margin
            df = df.withColumn(
                'cost_amount',
                spark_round(col('quantity') * col('unit_price') * lit(cost_ratio), 2)
            )
            
            df = df.withColumn(
                'profit_margin',
                when(col('net_amount') > 0,
                     spark_round(((col('net_amount') - col('cost_amount')) / col('net_amount')) * 100, 2))
                .otherwise(lit(0.00))
            )
            
            # Categorize sales using when/otherwise
            df = df.withColumn(
                'category',
                when(col('gross_amount') >= category_high, lit('HIGH'))
                .when(col('gross_amount') >= category_medium, lit('MEDIUM'))
                .otherwise(lit('LOW'))
            )
            
            # Generate analytics ID
            df = df.withColumn(
                'analytics_id',
                concat(
                    lit('ANL'),
                    col('trans_id'),
                    date_format(current_timestamp(), 'HHmmss')
                )
            )
            
            # Add ETL metadata
            df = df.withColumn('etl_run_id', lit(self.etl_run_id))
            df = df.withColumn('loaded_at', current_timestamp())
            
            # Select final columns
            analytics_df = df.select(
                'analytics_id',
                'trans_date',
                'customer_id',
                'product_id',
                col('quantity').alias('total_quantity'),
                'gross_amount',
                'net_amount',
                'discount_amount',
                'tax_amount',
                'currency',
                'sales_rep',
                'region',
                'profit_margin',
                'category',
                'etl_run_id',
                'loaded_at'
            )
            
            # Validate and cache
            analytics_df.cache()
            
            # Collect statistics
            record_count = analytics_df.count()
            
            # Calculate category distribution
            category_dist = analytics_df.groupBy('category').count().collect()
            category_breakdown = {row['category']: row['count'] for row in category_dist}
            
            stats = {
                'records_transformed': record_count,
                'category_breakdown': category_breakdown,
                'transformation_rules_applied': {
                    'discount_tiers': 2,
                    'tax_rate': tax_rate,
                    'cost_ratio': cost_ratio
                }
            }
            
            self.logger.info(f"Transformed {record_count} records successfully")
            self.logger.info(f"Category breakdown: {category_breakdown}")
            
            return analytics_df, stats
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def validate_transformed_data(self, df: DataFrame) -> Tuple[bool, list]:
        """
        Validate transformed data for data quality.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (validation passed, list of validation errors)
        """
        errors = []
        
        try:
            # Check for nulls in required fields
            required_fields = ['analytics_id', 'customer_id', 'product_id', 'gross_amount']
            for field in required_fields:
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    errors.append(f"Found {null_count} null values in {field}")
            
            # Check for negative amounts
            negative_amounts = df.filter(
                (col('gross_amount') < 0) | 
                (col('net_amount') < 0)
            ).count()
            if negative_amounts > 0:
                errors.append(f"Found {negative_amounts} records with negative amounts")
            
            # Check category values
            invalid_categories = df.filter(
                ~col('category').isin('HIGH', 'MEDIUM', 'LOW')
            ).count()
            if invalid_categories > 0:
                errors.append(f"Found {invalid_categories} records with invalid category")
            
            validation_passed = len(errors) == 0
            
            if validation_passed:
                self.logger.info("Data validation passed")
            else:
                self.logger.warning(f"Data validation failed: {errors}")
            
            return validation_passed, errors
            
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            return False, [str(e)]


def create_transformer(config: dict, logger: logging.Logger, etl_run_id: str) -> SalesDataTransformer:
    """Factory function to create transformer instance."""
    return SalesDataTransformer(config, logger, etl_run_id)