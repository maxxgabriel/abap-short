"""
Data Transformation Component
Migrated from ZCL_ETL_TRANSFORMER
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, round as spark_round,
    concat, current_timestamp, date_format
)
from typing import Optional

from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLTransformer:
    """
    Transform raw sales data into analytics format
    Migrated from ZCL_ETL_TRANSFORMER
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
        self.constants = ETLConstants()
        
    def transform_data(self, raw_df: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            DataFrame with transformed analytics data or None on error
        """
        try:
            self.logger.log_message(
                step=ETLConstants.STEP.TRANSFORM,
                status=ETLConstants.STATUS.SUCCESS,
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            analytics_df = self._apply_transformations(raw_df)
            
            output_count = analytics_df.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step=ETLConstants.STEP.TRANSFORM,
                status=ETLConstants.STATUS.SUCCESS,
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.STEP.TRANSFORM,
                status=ETLConstants.STATUS.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return None
            
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply business logic transformations"""
        
        # Get business rules from config
        discount_tier1_qty = self.constants.get('business_rules.discount.tier1_quantity', 10)
        discount_tier2_qty = self.constants.get('business_rules.discount.tier2_quantity', 15)
        discount_tier1_rate = self.constants.get('business_rules.discount.tier1_rate', 0.05)
        discount_tier2_rate = self.constants.get('business_rules.discount.tier2_rate', 0.10)
        tax_rate = self.constants.get('business_rules.tax_rate', 0.08)
        cost_ratio = self.constants.get('business_rules.cost_ratio', 0.60)
        
        # Calculate gross amount
        df = df.withColumn(
            'gross_amount',
            spark_round(col('quantity') * col('unit_price'), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            'discount_amount',
            when(col('quantity') > discount_tier2_qty, 
                 col('gross_amount') * discount_tier2_rate)
            .when(col('quantity') > discount_tier1_qty,
                  col('gross_amount') * discount_tier1_rate)
            .otherwise(0)
        )
        
        # Calculate tax on discounted amount
        df = df.withColumn(
            'tax_amount',
            spark_round((col('gross_amount') - col('discount_amount')) * tax_rate, 2)
        )
        
        # Calculate net amount
        df = df.withColumn(
            'net_amount',
            spark_round(
                col('gross_amount') - col('discount_amount') + col('tax_amount'),
                2
            )
        )
        
        # Calculate profit margin
        df = df.withColumn(
            'cost',
            col('quantity') * col('unit_price') * cost_ratio
        )
        df = df.withColumn(
            'profit_margin',
            when(col('net_amount') > 0,
                 spark_round(((col('net_amount') - col('cost')) / col('net_amount')) * 100, 2))
            .otherwise(0)
        )
        
        # Categorize sales
        df = self._categorize_sales(df)
        
        # Generate analytics ID
        df = df.withColumn(
            'analytics_id',
            concat(
                lit(ETLConstants.PREFIX.ANALYTICS_ID),
                col('trans_id'),
                date_format(current_timestamp(), 'HHmmss')
            )
        )
        
        # Add ETL metadata
        df = df.withColumn('etl_run_id', lit(self.logger.get_etl_run_id()))
        df = df.withColumn('loaded_at', current_timestamp())
        df = df.withColumn('loaded_by', lit('ETL_SYSTEM'))
        
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
            'loaded_at',
            'loaded_by'
        )
        
        return analytics_df
        
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """Categorize sales based on gross amount"""
        high_threshold = self.constants.get('business_rules.category.high_threshold', 2000.00)
        medium_threshold = self.constants.get('business_rules.category.medium_threshold', 500.00)
        
        return df.withColumn(
            'category',
            when(col('gross_amount') >= high_threshold, ETLConstants.CATEGORY.HIGH)
            .when(col('gross_amount') >= medium_threshold, ETLConstants.CATEGORY.MEDIUM)
            .otherwise(ETLConstants.CATEGORY.LOW)
        )