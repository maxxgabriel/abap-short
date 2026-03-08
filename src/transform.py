"""
ETL Transformer Module
Transforms raw sales data into analytics format.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp, 
    date_format, unix_timestamp
)
from pyspark.sql.types import DecimalType
from typing import Dict, Any

from src.logger import ETLLogger
from src.exceptions import ETLTransformError


class ETLTransformer:
    """Transforms raw sales data into analytics format with business rules."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict[str, Any]):
        """
        Initialize the ETL Transformer.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = config.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = config.get('discount_rate_tier2', 0.10)
        self.tax_rate = config.get('tax_rate', 0.08)
        self.cost_ratio = config.get('cost_ratio', 0.60)
        self.category_high_threshold = config.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = config.get('category_medium_threshold', 500.00)
        
    def transform_data(
        self,
        raw_df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            total_count = raw_df.count()
            
            # Calculate analytics fields
            analytics_df = self._calculate_analytics(raw_df, etl_run_id)
            
            success_count = analytics_df.count()
            error_count = total_count - success_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message=f'Transformed {success_count} of {total_count} records',
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            raise ETLTransformError(f'Data transformation failed: {str(e)}')
    
    def _calculate_analytics(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Calculate all analytics fields with business rules.
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with calculated analytics fields
        """
        # Calculate gross amount
        df = df.withColumn(
            'gross_amount',
            (col('quantity') * col('unit_price')).cast(DecimalType(16, 2))
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            'discount_amount',
            when(col('quantity') > self.discount_qty_tier2,
                 col('gross_amount') * lit(self.discount_rate_tier2))
            .when(col('quantity') > self.discount_qty_tier1,
                  col('gross_amount') * lit(self.discount_rate_tier1))
            .otherwise(lit(0.0))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate tax (on gross - discount)
        df = df.withColumn(
            'tax_amount',
            ((col('gross_amount') - col('discount_amount')) * lit(self.tax_rate))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate net amount
        df = df.withColumn(
            'net_amount',
            (col('gross_amount') - col('discount_amount') + col('tax_amount'))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate profit margin
        # Cost = quantity * unit_price * cost_ratio
        # Profit = net_amount - cost
        # Profit margin = (profit / net_amount) * 100
        df = df.withColumn(
            'cost_amount',
            (col('quantity') * col('unit_price') * lit(self.cost_ratio))
            .cast(DecimalType(16, 2))
        )
        
        df = df.withColumn(
            'profit_margin',
            when(col('net_amount') > 0,
                 (((col('net_amount') - col('cost_amount')) / col('net_amount')) * lit(100))
                 .cast(DecimalType(5, 2)))
            .otherwise(lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            'category',
            when(col('gross_amount') >= self.category_high_threshold, lit('HIGH'))
            .when(col('gross_amount') >= self.category_medium_threshold, lit('MEDIUM'))
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
        
        # Add ETL run ID
        df = df.withColumn('etl_run_id', lit(etl_run_id))
        
        # Add total_quantity (same as quantity in this context)
        df = df.withColumn('total_quantity', col('quantity'))
        
        # Select final columns in order
        analytics_df = df.select(
            'analytics_id',
            'trans_id',
            'trans_date',
            'customer_id',
            'product_id',
            'total_quantity',
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
        
        return analytics_df