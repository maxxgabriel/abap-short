"""
PySpark ETL Transformer Module

Migrated from ABAP ZCL_ETL_TRANSFORMER class.
Transforms raw sales data into analytics format using DataFrame operations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from typing import Tuple
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.schemas import RawSalesSchema, AnalyticsSchema


class ETLTransformer:
    """
    Transforms raw sales data into analytics format.
    Replaces ABAP LOOP AT patterns with DataFrame functional transformations.
    """
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize transformer with logger and Spark session.
        
        Args:
            logger: ETL logger instance
            spark: Active SparkSession
        """
        self.logger = logger
        self.spark = spark
        self.log = logging.getLogger(__name__)
        
    def transform_data(
        self, 
        raw_data: DataFrame
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Migrated from ABAP transform_data method.
        Replaces LOOP AT with DataFrame transformations.
        
        Args:
            raw_data: DataFrame with raw sales data
            
        Returns:
            Tuple of (transformed DataFrame, success boolean)
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            input_count = raw_data.count()
            self.log.info(f"Transforming {input_count} records")
            
            # Apply transformations using DataFrame operations
            analytics_df = self._calculate_analytics(raw_data)
            
            # Add metadata columns
            analytics_df = self._add_metadata_columns(analytics_df)
            
            # Validate transformed data
            analytics_df = analytics_df.filter(
                F.col('gross_amount').isNotNull() &
                (F.col('gross_amount') > 0)
            )
            
            output_count = analytics_df.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f'Transformed {output_count} of {input_count} records'
            )
            
            return analytics_df, True
            
        except Exception as e:
            self.log.error(f"Transformation failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            return self.spark.createDataFrame([], AnalyticsSchema.get_schema()), False
    
    def _calculate_analytics(self, raw_data: DataFrame) -> DataFrame:
        """
        Calculate all analytics fields using DataFrame operations.
        
        Migrated from ABAP calculate_analytics method.
        Replaces row-by-row processing with vectorized operations.
        
        Args:
            raw_data: Raw sales DataFrame
            
        Returns:
            DataFrame with calculated analytics fields
        """
        # Calculate gross amount
        df = raw_data.withColumn(
            'gross_amount',
            F.col('quantity') * F.col('unit_price')
        )
        
        # Calculate discount based on quantity tiers
        # Migrated from ABAP IF-ELSEIF logic for discount calculation
        df = df.withColumn(
            'discount_amount',
            F.when(F.col('quantity') > 15, F.col('gross_amount') * 0.10)
            .when(F.col('quantity') > 10, F.col('gross_amount') * 0.05)
            .otherwise(0.0)
        )
        
        # Calculate tax (8% on gross - discount)
        df = df.withColumn(
            'tax_amount',
            (F.col('gross_amount') - F.col('discount_amount')) * 0.08
        )
        
        # Calculate net amount
        df = df.withColumn(
            'net_amount',
            F.col('gross_amount') - F.col('discount_amount') + F.col('tax_amount')
        )
        
        # Calculate cost (60% of unit price * quantity)
        df = df.withColumn(
            'cost_amount',
            F.col('quantity') * F.col('unit_price') * 0.60
        )
        
        # Calculate profit margin as percentage
        df = df.withColumn(
            'profit_margin',
            F.when(
                F.col('net_amount') > 0,
                ((F.col('net_amount') - F.col('cost_amount')) / F.col('net_amount') * 100)
            ).otherwise(0.0)
        )
        
        # Categorize sales
        df = df.withColumn(
            'category',
            self._categorize_sale_udf()
        )
        
        # Generate analytics ID
        df = df.withColumn(
            'analytics_id',
            F.concat(
                F.lit('ANL'),
                F.col('trans_id'),
                F.date_format(F.current_timestamp(), 'HHmmss')
            )
        )
        
        # Select and rename columns to match analytics schema
        analytics_df = df.select(
            F.col('analytics_id'),
            F.col('trans_date'),
            F.col('customer_id'),
            F.col('product_id'),
            F.col('quantity').alias('total_quantity'),
            F.col('gross_amount'),
            F.col('net_amount'),
            F.col('discount_amount'),
            F.col('tax_amount'),
            F.col('currency'),
            F.col('sales_rep'),
            F.col('region'),
            F.col('profit_margin'),
            F.col('category')
        )
        
        return analytics_df
    
    def _categorize_sale_udf(self) -> F.Column:
        """
        Categorize sales based on gross amount.
        
        Migrated from ABAP categorize_sale method.
        
        Returns:
            Column expression for categorization
        """
        return F.when(F.col('gross_amount') >= 2000, 'HIGH') \
                .when(F.col('gross_amount') >= 500, 'MEDIUM') \
                .otherwise('LOW')
    
    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata columns to the DataFrame.
        
        Args:
            df: DataFrame to add metadata to
            
        Returns:
            DataFrame with metadata columns added
        """
        return df.withColumn(
            'etl_run_id',
            F.lit(self.logger.get_etl_run_id())
        ).withColumn(
            'loaded_at',
            F.current_timestamp()
        ).withColumn(
            'loaded_by',
            F.lit('pyspark_etl')
        )