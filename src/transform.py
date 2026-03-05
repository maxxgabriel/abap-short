"""
Data transformation module for Sales ETL pipeline.
Applies business rules and calculates analytics metrics.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, when, lit, round as spark_round
from typing import Tuple
import logging


class SalesDataTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.business_rules = config.get('business_rules', {})
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        
        Returns:
            StructType schema definition
        """
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
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame, etl_run_id: str) -> Tuple[DataFrame, dict]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: Unique ETL run identifier
            
        Returns:
            Tuple of (transformed DataFrame, metrics dictionary)
        """
        try:
            self.logger.info("Starting data transformation")
            
            initial_count = raw_df.count()
            
            # Calculate business metrics
            transformed_df = self._apply_business_rules(raw_df, etl_run_id)
            
            # Calculate final metrics
            final_count = transformed_df.count()
            
            metrics = {
                'records_transformed': final_count,
                'records_input': initial_count,
                'records_success': final_count,
                'records_error': initial_count - final_count
            }
            
            self.logger.info(f"Transformed {final_count} of {initial_count} records")
            
            return transformed_df, metrics
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_business_rules(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Apply business rules to calculate analytics fields.
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame
        """
        # Get business rule parameters
        discount_qty_tier1 = self.business_rules.get('discount_qty_tier1', 10)
        discount_qty_tier2 = self.business_rules.get('discount_qty_tier2', 15)
        discount_rate_tier1 = self.business_rules.get('discount_rate_tier1', 0.05)
        discount_rate_tier2 = self.business_rules.get('discount_rate_tier2', 0.10)
        tax_rate = self.business_rules.get('tax_rate', 0.08)
        cost_ratio = self.business_rules.get('cost_ratio', 0.60)
        category_high = self.business_rules.get('category_high_threshold', 2000.00)
        category_medium = self.business_rules.get('category_medium_threshold', 500.00)
        
        # Calculate gross amount
        df = df.withColumn('gross_amount', col('quantity') * col('unit_price'))
        
        # Calculate discount based on quantity tiers
        df = df.withColumn('discount_amount',
            when(col('quantity') > discount_qty_tier2, 
                 col('gross_amount') * lit(discount_rate_tier2))
            .when(col('quantity') > discount_qty_tier1,
                  col('gross_amount') * lit(discount_rate_tier1))
            .otherwise(lit(0.0))
        )
        
        # Calculate tax (on gross - discount)
        df = df.withColumn('tax_amount',
            (col('gross_amount') - col('discount_amount')) * lit(tax_rate)
        )
        
        # Calculate net amount
        df = df.withColumn('net_amount',
            col('gross_amount') - col('discount_amount') + col('tax_amount')
        )
        
        # Calculate profit margin (simplified: cost is cost_ratio of unit price)
        df = df.withColumn('cost_amount',
            col('quantity') * col('unit_price') * lit(cost_ratio)
        )
        
        df = df.withColumn('profit_margin',
            spark_round(
                when(col('net_amount') > 0,
                     ((col('net_amount') - col('cost_amount')) / col('net_amount')) * 100
                ).otherwise(lit(0.0)),
                2
            )
        )
        
        # Categorize sales
        df = df.withColumn('category',
            when(col('gross_amount') >= category_high, lit('HIGH'))
            .when(col('gross_amount') >= category_medium, lit('MEDIUM'))
            .otherwise(lit('LOW'))
        )
        
        # Generate analytics ID
        df = df.withColumn('analytics_id',
            lit('ANL_') + col('trans_id')
        )
        
        # Add ETL run ID
        df = df.withColumn('etl_run_id', lit(etl_run_id))
        
        # Rename quantity column
        df = df.withColumn('total_quantity', col('quantity'))
        
        # Select final columns
        final_df = df.select(
            'analytics_id',
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
        
        return final_df
    
    def validate_transformed_data(self, df: DataFrame) -> Tuple[bool, list]:
        """
        Validate transformed data quality.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (validation success boolean, list of validation messages)
        """
        validation_messages = []
        
        # Check for negative net amounts
        negative_net = df.filter(col('net_amount') < 0).count()
        if negative_net > 0:
            validation_messages.append(f"Found {negative_net} records with negative net amounts")
        
        # Check for invalid categories
        invalid_category = df.filter(
            ~col('category').isin(['HIGH', 'MEDIUM', 'LOW'])
        ).count()
        if invalid_category > 0:
            validation_messages.append(f"Found {invalid_category} records with invalid categories")
        
        # Check for unrealistic profit margins (< -100% or > 100%)
        invalid_margin = df.filter(
            (col('profit_margin') < -100) | (col('profit_margin') > 100)
        ).count()
        if invalid_margin > 0:
            validation_messages.append(f"Found {invalid_margin} records with unrealistic profit margins")
        
        is_valid = len(validation_messages) == 0
        
        if is_valid:
            self.logger.info("Transformation validation passed")
        else:
            self.logger.warning(f"Transformation validation issues: {validation_messages}")
        
        return is_valid, validation_messages