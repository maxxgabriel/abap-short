"""
ETL Transformer
Migrated from ZCL_ETL_TRANSFORMER
"""

from typing import List, Dict
from decimal import Decimal
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, when, lit

from src.logger import ETLLogger
from src.constants import ETLConstants


class ETLTransformer:
    """Transforms raw sales data into analytics format"""
    
    def __init__(self, logger: ETLLogger, config: Dict):
        """
        Initialize transformer
        
        Args:
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
        self.constants = ETLConstants()
        self.spark = self._get_spark_session()
    
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        return SparkSession.builder \
            .appName("ETL_Transformer") \
            .getOrCreate()
    
    def transform_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform raw sales data
        
        Args:
            raw_data: List of raw sales records
            
        Returns:
            List of transformed analytics records
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            # Convert to DataFrame
            df = self._create_dataframe(raw_data)
            
            # Apply transformations
            df_transformed = self._apply_transformations(df)
            
            # Convert back to list of dicts
            analytics_data = [row.asDict() for row in df_transformed.collect()]
            
            success_count = len(analytics_data)
            total_count = len(raw_data)
            error_count = total_count - success_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f'Transformed {success_count} of {total_count} records'
            )
            
            return analytics_data
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            raise
    
    def _create_dataframe(self, raw_data: List[Dict]) -> DataFrame:
        """Create DataFrame from raw data"""
        schema = StructType([
            StructField("trans_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), False),
            StructField("region", StringType(), False),
            StructField("status", StringType(), False)
        ])
        
        df = self.spark.createDataFrame(raw_data, schema)
        return df
    
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformations
        
        Args:
            df: Raw data DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Calculate gross amount
        df = df.withColumn(
            'gross_amount',
            col('quantity') * col('unit_price')
        )
        
        # Calculate discount based on quantity
        df = df.withColumn(
            'discount_amount',
            when(col('quantity') > self.constants.DISCOUNT_QTY_TIER2,
                 col('gross_amount') * lit(self.constants.DISCOUNT_RATE_TIER2))
            .when(col('quantity') > self.constants.DISCOUNT_QTY_TIER1,
                  col('gross_amount') * lit(self.constants.DISCOUNT_RATE_TIER1))
            .otherwise(lit(0))
        )
        
        # Calculate tax
        df = df.withColumn(
            'tax_amount',
            (col('gross_amount') - col('discount_amount')) * lit(self.constants.TAX_RATE)
        )
        
        # Calculate net amount
        df = df.withColumn(
            'net_amount',
            col('gross_amount') - col('discount_amount') + col('tax_amount')
        )
        
        # Calculate cost and profit margin
        df = df.withColumn(
            'cost',
            col('quantity') * col('unit_price') * lit(self.constants.COST_RATIO)
        )
        
        df = df.withColumn(
            'profit_margin',
            when(col('net_amount') > 0,
                 ((col('net_amount') - col('cost')) / col('net_amount')) * lit(100))
            .otherwise(lit(0))
        )
        
        # Categorize sales
        df = df.withColumn(
            'category',
            when(col('gross_amount') >= self.constants.CATEGORY_HIGH_THRESHOLD,
                 lit(self.constants.CATEGORY_HIGH))
            .when(col('gross_amount') >= self.constants.CATEGORY_MEDIUM_THRESHOLD,
                  lit(self.constants.CATEGORY_MEDIUM))
            .otherwise(lit(self.constants.CATEGORY_LOW))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            'analytics_id',
            self._generate_analytics_id_udf(col('trans_id'))
        )
        
        # Add ETL run ID
        df = df.withColumn(
            'etl_run_id',
            lit(self.logger.etl_run_id)
        )
        
        # Select and rename final columns
        df_final = df.select(
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
        
        return df_final
    
    def _generate_analytics_id_udf(self, trans_id_col):
        """Generate analytics ID (simplified - use UDF for complex logic)"""
        from pyspark.sql.functions import concat, lit, date_format, current_timestamp
        
        timestamp_str = date_format(current_timestamp(), 'HHmmss')
        return concat(lit('ANL'), trans_id_col, timestamp_str)