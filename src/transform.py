"""
PySpark Data Transformation Module
Transforms raw sales data into analytics format with business rules.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, expr, current_timestamp, concat_ws, monotonically_increasing_id
)
from pyspark.sql.types import DecimalType
from typing import Dict

from src.logger import ETLLogger
from src.exceptions import TransformationError


class DataTransformer:
    """
    Transforms raw sales data applying business rules for analytics.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict):
        """
        Initialize the data transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)
        self.etl_run_id = config.get('etl_run_id', 'UNKNOWN')
        
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            input_count = raw_df.count()
            
            # Calculate analytics fields
            df_transformed = raw_df.withColumn(
                'gross_amount',
                (col('quantity') * col('unit_price')).cast(DecimalType(16, 2))
            )
            
            # Calculate discount based on quantity tiers
            df_transformed = df_transformed.withColumn(
                'discount_amount',
                when(col('quantity') > self.discount_qty_tier2,
                     col('gross_amount') * lit(self.discount_rate_tier2))
                .when(col('quantity') > self.discount_qty_tier1,
                      col('gross_amount') * lit(self.discount_rate_tier1))
                .otherwise(lit(0))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate tax
            df_transformed = df_transformed.withColumn(
                'tax_amount',
                ((col('gross_amount') - col('discount_amount')) * lit(self.tax_rate))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate net amount
            df_transformed = df_transformed.withColumn(
                'net_amount',
                (col('gross_amount') - col('discount_amount') + col('tax_amount'))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate profit margin
            df_transformed = df_transformed.withColumn(
                'cost',
                (col('quantity') * col('unit_price') * lit(self.cost_ratio))
                .cast(DecimalType(16, 2))
            )
            
            df_transformed = df_transformed.withColumn(
                'profit_margin',
                (((col('net_amount') - col('cost')) / col('net_amount')) * lit(100))
                .cast(DecimalType(5, 2))
            )
            
            # Categorize sales
            df_transformed = df_transformed.withColumn(
                'category',
                when(col('gross_amount') >= self.category_high_threshold, lit('HIGH'))
                .when(col('gross_amount') >= self.category_medium_threshold, lit('MEDIUM'))
                .otherwise(lit('LOW'))
            )
            
            # Generate analytics ID
            df_transformed = df_transformed.withColumn(
                'analytics_id',
                concat_ws('_', lit('ANL'), col('trans_id'), 
                         expr("date_format(current_timestamp(), 'yyyyMMddHHmmss')"))
            )
            
            # Add ETL metadata
            df_transformed = df_transformed.withColumn(
                'etl_run_id',
                lit(self.etl_run_id)
            )
            
            # Select and rename columns for analytics output
            df_analytics = df_transformed.select(
                col('analytics_id'),
                col('trans_date'),
                col('customer_id'),
                col('product_id'),
                col('quantity').alias('total_quantity'),
                col('gross_amount'),
                col('net_amount'),
                col('discount_amount'),
                col('tax_amount'),
                col('currency'),
                col('sales_rep'),
                col('region'),
                col('profit_margin'),
                col('category'),
                col('etl_run_id')
            )
            
            output_count = df_analytics.count()
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                message=f'Transformed {output_count} of {input_count} records'
            )
            
            return df_analytics
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            raise TransformationError(f"Data transformation failed: {str(e)}") from e