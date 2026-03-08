"""
Data transformation module for Sales ETL pipeline.

Applies business rules and calculates analytics metrics.
"""

from typing import Optional, Tuple
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
import logging

from src.exceptions import TransformError
from src.logger import ETLLogger


class SalesTransformer:
    """
    Transforms raw sales data into analytics format.
    
    Applies business rules for discounts, taxes, profit margins,
    and categorization.
    """
    
    # Target analytics schema
    ANALYTICS_SCHEMA = StructType([
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
        StructField("profit_margin", DecimalType(5, 2), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("etl_run_id", StringType(), nullable=False)
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        etl_run_id: str,
        config: dict
    ):
        """
        Initialize transformer.
        
        Args:
            spark: Active Spark session
            logger: ETL logger instance
            etl_run_id: Current ETL run identifier
            config: Configuration parameters
        """
        self.spark = spark
        self.logger = logger
        self.etl_run_id = etl_run_id
        self.config = config
        self._log = logging.getLogger(__name__)
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = config.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = Decimal(str(config.get('discount_rate_tier1', 0.05)))
        self.discount_rate_tier2 = Decimal(str(config.get('discount_rate_tier2', 0.10)))
        self.tax_rate = Decimal(str(config.get('tax_rate', 0.08)))
        self.cost_ratio = Decimal(str(config.get('cost_ratio', 0.60)))
        self.category_high_threshold = Decimal(str(config.get('category_high_threshold', 2000.00)))
        self.category_medium_threshold = Decimal(str(config.get('category_medium_threshold', 500.00)))
    
    def transform_data(
        self,
        raw_df: DataFrame
    ) -> Tuple[DataFrame, int, int]:
        """
        Transform raw sales data.
        
        Args:
            raw_df: Raw sales DataFrame
        
        Returns:
            Tuple of (transformed_df, success_count, error_count)
        
        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            # Validate input
            self._validate_input_data(raw_df)
            
            # Apply transformations
            analytics_df = self._apply_transformations(raw_df)
            
            # Validate output
            self._validate_output_data(analytics_df)
            
            # Count results
            success_count = analytics_df.count()
            error_count = 0
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=success_count,
                records_success=success_count,
                message=f'Successfully transformed {success_count} records'
            )
            
            return analytics_df, success_count, error_count
            
        except TransformError:
            raise
        except Exception as e:
            error_msg = f'Transformation failed: {str(e)}'
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=error_msg
            )
            raise TransformError(
                message=error_msg,
                error_code='TRF_001',
                original_exception=e
            )
    
    def _apply_transformations(self, raw_df: DataFrame) -> DataFrame:
        """
        Apply business rule transformations.
        
        Args:
            raw_df: Raw sales DataFrame
        
        Returns:
            Transformed analytics DataFrame
        """
        from pyspark.sql import functions as F
        from pyspark.sql.functions import when, col, lit, concat, current_timestamp
        
        # Calculate gross amount
        df = raw_df.withColumn(
            'gross_amount',
            col('quantity') * col('unit_price')
        )
        
        # Calculate discount (tiered based on quantity)
        df = df.withColumn(
            'discount_amount',
            when(
                col('quantity') > self.discount_qty_tier2,
                col('gross_amount') * lit(float(self.discount_rate_tier2))
            ).when(
                col('quantity') > self.discount_qty_tier1,
                col('gross_amount') * lit(float(self.discount_rate_tier1))
            ).otherwise(lit(0.0))
        )
        
        # Calculate tax (on gross - discount)
        df = df.withColumn(
            'tax_amount',
            (col('gross_amount') - col('discount_amount')) * lit(float(self.tax_rate))
        )
        
        # Calculate net amount
        df = df.withColumn(
            'net_amount',
            col('gross_amount') - col('discount_amount') + col('tax_amount')
        )
        
        # Calculate cost and profit margin
        df = df.withColumn(
            'cost',
            col('quantity') * col('unit_price') * lit(float(self.cost_ratio))
        )
        
        df = df.withColumn(
            'profit_margin',
            when(
                col('net_amount') > 0,
                ((col('net_amount') - col('cost')) / col('net_amount') * 100)
            ).otherwise(lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            'category',
            when(
                col('gross_amount') >= lit(float(self.category_high_threshold)),
                lit('HIGH')
            ).when(
                col('gross_amount') >= lit(float(self.category_medium_threshold)),
                lit('MEDIUM')
            ).otherwise(lit('LOW'))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            'analytics_id',
            concat(
                lit('ANL'),
                col('trans_id'),
                F.date_format(current_timestamp(), 'HHmmss')
            )
        )
        
        # Add ETL run ID
        df = df.withColumn('etl_run_id', lit(self.etl_run_id))
        
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
            'etl_run_id'
        )
        
        return analytics_df
    
    def _validate_input_data(self, df: DataFrame) -> None:
        """
        Validate input data before transformation.
        
        Args:
            df: Input DataFrame
        
        Raises:
            TransformError: If validation fails
        """
        from pyspark.sql import functions as F
        
        # Check for negative quantities
        negative_qty = df.filter(col('quantity') < 0).count()
        if negative_qty > 0:
            raise TransformError(
                message=f'Found {negative_qty} records with negative quantity',
                transformation_rule='positive_quantity',
                error_code='TRF_002',
                metadata={'negative_count': negative_qty}
            )
        
        # Check for negative prices
        negative_price = df.filter(col('unit_price') < 0).count()
        if negative_price > 0:
            raise TransformError(
                message=f'Found {negative_price} records with negative price',
                transformation_rule='positive_price',
                error_code='TRF_003',
                metadata={'negative_count': negative_price}
            )
    
    def _validate_output_data(self, df: DataFrame) -> None:
        """
        Validate transformed data.
        
        Args:
            df: Output DataFrame
        
        Raises:
            TransformError: If validation fails
        """
        from pyspark.sql import functions as F
        
        # Check for invalid categories
        valid_categories = ['HIGH', 'MEDIUM', 'LOW']
        invalid_categories = df.filter(
            ~col('category').isin(valid_categories)
        ).count()
        
        if invalid_categories > 0:
            raise TransformError(
                message=f'Found {invalid_categories} records with invalid category',
                transformation_rule='valid_category',
                error_code='TRF_004',
                metadata={
                    'invalid_count': invalid_categories,
                    'valid_categories': valid_categories
                }
            )