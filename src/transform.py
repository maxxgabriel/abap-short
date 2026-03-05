"""
ETL Transformer Module - PySpark DataFrame Implementation
Transforms raw sales data into analytics format
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ETLTransformError


class ETLTransformer:
    """
    PySpark implementation of ETL Transformer component
    Applies business rules and calculations to raw data
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize ETL Transformer
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("analytics_id", StringType(), False),
            StructField("trans_id", StringType(), False),
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
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed_df, success_flag)
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations
            transformed_df = self._calculate_analytics(raw_df)
            
            final_count = transformed_df.count()
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=initial_count,
                records_success=final_count,
                message=f"Transformed {final_count} of {initial_count} records"
            )
            
            return transformed_df, True
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f"Transformation failed: {str(e)}"
            )
            raise ETLTransformError(f"Data transformation failed: {str(e)}") from e
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Apply business calculations
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame with analytics
        """
        # Calculate gross amount
        df = df.withColumn('gross_amount', col('quantity') * col('unit_price'))
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            'discount_amount',
            when(col('quantity') > self.discount_qty_tier2, 
                 col('gross_amount') * self.discount_rate_tier2)
            .when(col('quantity') > self.discount_qty_tier1,
                  col('gross_amount') * self.discount_rate_tier1)
            .otherwise(0.0)
        )
        
        # Calculate tax amount (on gross - discount)
        df = df.withColumn(
            'tax_amount',
            (col('gross_amount') - col('discount_amount')) * self.tax_rate
        )
        
        # Calculate net amount
        df = df.withColumn(
            'net_amount',
            col('gross_amount') - col('discount_amount') + col('tax_amount')
        )
        
        # Calculate profit margin
        df = df.withColumn('cost_amount', col('quantity') * col('unit_price') * self.cost_ratio)
        df = df.withColumn(
            'profit_margin',
            ((col('net_amount') - col('cost_amount')) / col('net_amount') * 100)
        )
        
        # Categorize sales
        df = df.withColumn(
            'category',
            when(col('gross_amount') >= self.category_high_threshold, 'HIGH')
            .when(col('gross_amount') >= self.category_medium_threshold, 'MEDIUM')
            .otherwise('LOW')
        )
        
        # Generate analytics ID
        df = df.withColumn(
            'analytics_id',
            lit('ANL').concat(col('trans_id')).concat(lit(self._get_timestamp_suffix()))
        )
        
        # Add ETL run ID
        df = df.withColumn('etl_run_id', lit(self.logger.get_etl_run_id()))
        
        # Select and rename columns
        return df.select(
            'analytics_id',
            'trans_id',
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
            'etl_run_id'
        )
    
    def _get_timestamp_suffix(self) -> str:
        """Generate timestamp suffix for IDs"""
        from datetime import datetime
        return datetime.now().strftime('%H%M%S')