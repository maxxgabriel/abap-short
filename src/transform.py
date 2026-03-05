"""
Data Transformer Module
Transforms raw sales data into analytics format with business logic.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, concat, current_timestamp, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Optional
import logging


class DataTransformer:
    """
    Transforms raw sales data into analytics format with calculated metrics.
    Applies business rules for discounts, taxes, and categorization.
    """
    
    def __init__(self, logger: logging.Logger, config: dict):
        """
        Initialize the DataTransformer with dependencies.
        
        Args:
            logger: Logger instance for logging transformation activities
            config: Configuration dictionary with business rules
        """
        self.logger = logger
        self.config = config
        self.business_rules = self._load_business_rules()
        self.schema = self._define_schema()
        
    def _load_business_rules(self) -> dict:
        """
        Load business rules from configuration.
        
        Returns:
            Dictionary containing business rules
        """
        return {
            'discount_qty_tier1': self.config.get('discount_qty_tier1', 10),
            'discount_qty_tier2': self.config.get('discount_qty_tier2', 15),
            'discount_rate_tier1': self.config.get('discount_rate_tier1', 0.05),
            'discount_rate_tier2': self.config.get('discount_rate_tier2', 0.10),
            'tax_rate': self.config.get('tax_rate', 0.08),
            'cost_ratio': self.config.get('cost_ratio', 0.60),
            'category_high_threshold': self.config.get('category_high_threshold', 2000.00),
            'category_medium_threshold': self.config.get('category_medium_threshold', 500.00),
        }
    
    def _define_schema(self) -> StructType:
        """
        Define the schema for analytics data.
        
        Returns:
            StructType schema for analytics records
        """
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
            StructField("loaded_at", TimestampType(), nullable=False)
        ])
    
    def transform_data(
        self, 
        df_raw: DataFrame, 
        etl_run_id: str
    ) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame or None if transformation fails
        """
        try:
            self.logger.info("Starting data transformation")
            
            initial_count = df_raw.count()
            
            # Calculate analytics metrics
            df_transformed = self._calculate_analytics(df_raw, etl_run_id)
            
            # Validate transformed data
            df_validated = self._validate_transformed_data(df_transformed)
            
            success_count = df_validated.count()
            error_count = initial_count - success_count
            
            self.logger.info(
                f"Transformation completed. Success: {success_count}, Errors: {error_count}",
                extra={
                    'step': 'TRANSFORM',
                    'status': 'SUCCESS',
                    'records_processed': initial_count,
                    'records_success': success_count,
                    'records_error': error_count
                }
            )
            
            return df_validated
            
        except Exception as e:
            self.logger.error(
                f"Transformation failed: {str(e)}",
                extra={'step': 'TRANSFORM', 'status': 'ERROR'},
                exc_info=True
            )
            return None
    
    def _calculate_analytics(self, df_raw: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Calculate all analytics metrics for each record.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with calculated analytics
        """
        rules = self.business_rules
        
        # Calculate gross amount
        df = df_raw.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > rules['discount_qty_tier2'],
                 spark_round(col("gross_amount") * lit(rules['discount_rate_tier2']), 2))
            .when(col("quantity") > rules['discount_qty_tier1'],
                  spark_round(col("gross_amount") * lit(rules['discount_rate_tier1']), 2))
            .otherwise(lit(0.00))
        )
        
        # Calculate tax amount (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * lit(rules['tax_rate']), 2)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            spark_round(col("gross_amount") - col("discount_amount") + col("tax_amount"), 2)
        )
        
        # Calculate cost and profit margin
        df = df.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * lit(rules['cost_ratio']), 2)
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2))
            .otherwise(lit(0.00))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= rules['category_high_threshold'], lit("HIGH"))
            .when(col("gross_amount") >= rules['category_medium_threshold'], lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(lit("ANL"), col("trans_id"), lit("_"), current_timestamp().cast("string"))
        )
        
        # Add metadata
        df = df.withColumn("etl_run_id", lit(etl_run_id))
        df = df.withColumn("loaded_at", current_timestamp())
        df = df.withColumn("total_quantity", col("quantity"))
        
        # Select final columns
        analytics_df = df.select(
            "analytics_id",
            "trans_date",
            "customer_id",
            "product_id",
            "total_quantity",
            "gross_amount",
            "net_amount",
            "discount_amount",
            "tax_amount",
            "currency",
            "sales_rep",
            "region",
            "profit_margin",
            "category",
            "etl_run_id",
            "loaded_at"
        )
        
        return analytics_df
    
    def _validate_transformed_data(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed data before loading.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Validated DataFrame with invalid records filtered out
        """
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return df_valid
    
    def get_transformation_summary(self, df: DataFrame) -> dict:
        """
        Generate summary statistics for transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Dictionary containing transformation summary
        """
        try:
            from pyspark.sql.functions import sum as spark_sum, avg, count
            
            summary = df.agg(
                count("*").alias("total_records"),
                spark_sum("gross_amount").alias("total_gross"),
                spark_sum("net_amount").alias("total_net"),
                spark_sum("discount_amount").alias("total_discount"),
                avg("profit_margin").alias("avg_profit_margin")
            ).collect()[0]
            
            category_counts = df.groupBy("category").count().collect()
            
            return {
                'total_records': summary['total_records'],
                'total_gross': float(summary['total_gross']) if summary['total_gross'] else 0,
                'total_net': float(summary['total_net']) if summary['total_net'] else 0,
                'total_discount': float(summary['total_discount']) if summary['total_discount'] else 0,
                'avg_profit_margin': float(summary['avg_profit_margin']) if summary['avg_profit_margin'] else 0,
                'category_distribution': {row['category']: row['count'] for row in category_counts}
            }
        except Exception as e:
            self.logger.warning(f"Failed to generate transformation summary: {str(e)}")
            return {}