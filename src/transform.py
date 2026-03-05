"""
Data Transformation Module
Transforms raw sales data into analytics format with business rules
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, round as spark_round
)
from typing import Optional, Tuple
import logging

from src.exceptions import ETLTransformationError
from src.logger import ETLLogger


class AnalyticsSchema:
    """Schema definition for analytics data"""
    
    @staticmethod
    def get_schema() -> StructType:
        """Returns the StructType schema for analytics data"""
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=False),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=False),
            StructField("discount_amount", DecimalType(16, 2), nullable=False),
            StructField("tax_amount", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=False),
            StructField("loaded_by", StringType(), nullable=True)
        ])


class SalesDataTransformer:
    """Transforms raw sales data into analytics format"""
    
    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        config: dict,
        etl_run_id: str
    ):
        """
        Initialize the transformer
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.etl_run_id = etl_run_id
        self.business_rules = config.get('business_rules', {})
        
    def transform_data(
        self,
        raw_df: DataFrame
    ) -> Tuple[Optional[DataFrame], bool]:
        """
        Transform raw sales data to analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed_df, success_flag)
            
        Raises:
            ETLTransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            df = self._calculate_amounts(raw_df)
            df = self._calculate_discount(df)
            df = self._calculate_tax(df)
            df = self._calculate_net_amount(df)
            df = self._calculate_profit_margin(df)
            df = self._categorize_sales(df)
            df = self._generate_analytics_id(df)
            df = self._add_metadata(df)
            
            # Select final columns in schema order
            df = self._select_final_columns(df)
            
            # Validate output
            df = self._validate_transformed_data(df)
            
            output_count = df.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f'Transformed {output_count} of {input_count} records'
            )
            
            return df, True
            
        except Exception as e:
            error_msg = f'Transformation failed: {str(e)}'
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=error_msg
            )
            logging.error(error_msg, exc_info=True)
            raise ETLTransformationError(error_msg) from e
    
    def _calculate_amounts(self, df: DataFrame) -> DataFrame:
        """Calculate gross amount"""
        return df.withColumn(
            'gross_amount',
            spark_round(col('quantity') * col('unit_price'), 2)
        )
    
    def _calculate_discount(self, df: DataFrame) -> DataFrame:
        """
        Calculate discount based on quantity tiers
        Tier 1: quantity > 10 -> 5% discount
        Tier 2: quantity > 15 -> 10% discount
        """
        discount_rules = self.business_rules.get('discount', {})
        tier1_qty = discount_rules.get('tier1', {}).get('quantity_threshold', 10)
        tier1_rate = discount_rules.get('tier1', {}).get('rate', 0.05)
        tier2_qty = discount_rules.get('tier2', {}).get('quantity_threshold', 15)
        tier2_rate = discount_rules.get('tier2', {}).get('rate', 0.10)
        
        return df.withColumn(
            'discount_amount',
            when(col('quantity') > tier2_qty, 
                 spark_round(col('gross_amount') * lit(tier2_rate), 2))
            .when(col('quantity') > tier1_qty,
                  spark_round(col('gross_amount') * lit(tier1_rate), 2))
            .otherwise(lit(0.0))
        )
    
    def _calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax on (gross - discount)
        Default tax rate: 8%
        """
        tax_rate = self.business_rules.get('tax', {}).get('rate', 0.08)
        
        return df.withColumn(
            'tax_amount',
            spark_round(
                (col('gross_amount') - col('discount_amount')) * lit(tax_rate),
                2
            )
        )
    
    def _calculate_net_amount(self, df: DataFrame) -> DataFrame:
        """Calculate net amount = gross - discount + tax"""
        return df.withColumn(
            'net_amount',
            spark_round(
                col('gross_amount') - col('discount_amount') + col('tax_amount'),
                2
            )
        )
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin percentage
        Assumes cost is 60% of unit price
        """
        cost_ratio = self.business_rules.get('cost', {}).get('ratio', 0.60)
        
        df = df.withColumn(
            'cost_amount',
            spark_round(col('quantity') * col('unit_price') * lit(cost_ratio), 2)
        )
        
        return df.withColumn(
            'profit_margin',
            when(col('net_amount') > 0,
                 spark_round(
                     ((col('net_amount') - col('cost_amount')) / col('net_amount')) * 100,
                     2
                 ))
            .otherwise(lit(0.0))
        )
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount
        HIGH: >= 2000
        MEDIUM: >= 500
        LOW: < 500
        """
        category_rules = self.business_rules.get('category', {})
        high_threshold = category_rules.get('high_threshold', 2000.0)
        medium_threshold = category_rules.get('medium_threshold', 500.0)
        
        return df.withColumn(
            'category',
            when(col('gross_amount') >= lit(high_threshold), lit('HIGH'))
            .when(col('gross_amount') >= lit(medium_threshold), lit('MEDIUM'))
            .otherwise(lit('LOW'))
        )
    
    def _generate_analytics_id(self, df: DataFrame) -> DataFrame:
        """Generate unique analytics ID"""
        from pyspark.sql.functions import concat, lpad, monotonically_increasing_id
        
        return df.withColumn(
            'analytics_id',
            concat(
                lit('ANL'),
                col('trans_id'),
                lpad(monotonically_increasing_id().cast('string'), 10, '0')
            )
        )
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata fields"""
        return df \
            .withColumn('etl_run_id', lit(self.etl_run_id)) \
            .withColumn('loaded_at', current_timestamp()) \
            .withColumn('loaded_by', lit('pyspark_etl'))
    
    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select columns in final schema order"""
        schema = AnalyticsSchema.get_schema()
        final_columns = [f.name for f in schema.fields]
        
        # Map source columns to target
        column_mapping = {
            'total_quantity': 'quantity',
            # other columns are already correctly named
        }
        
        # Rename if needed
        for target_col, source_col in column_mapping.items():
            if source_col in df.columns and target_col not in df.columns:
                df = df.withColumnRenamed(source_col, target_col)
        
        # Select only final columns
        available_columns = [c for c in final_columns if c in df.columns]
        return df.select(*available_columns)
    
    def _validate_transformed_data(self, df: DataFrame) -> DataFrame:
        """Validate transformed data and filter invalid records"""
        # Filter out records with invalid data
        df = df.filter(
            (col('analytics_id').isNotNull()) &
            (col('customer_id').isNotNull()) &
            (col('product_id').isNotNull()) &
            (col('gross_amount') > 0) &
            (col('currency').isNotNull()) &
            (col('category').isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        
        return df