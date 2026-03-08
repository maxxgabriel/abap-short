"""
ETL Transformation module with integrated logging
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.logger import ETLLogger


class SalesDataTransformer:
    """
    Transform sales data with correlation tracking
    Replaces ZCL_ETL_TRANSFORMER from ABAP
    """
    
    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize transformer
        
        Args:
            logger: ETL logger with correlation tracking
            config: Configuration dictionary
        """
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
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Transform raw sales data to analytics format
        
        Args:
            df: Input DataFrame with raw sales data
            
        Returns:
            Transformed DataFrame
        """
        try:
            input_count = df.count()
            self.logger.log_transform(
                message="Starting data transformation",
                records_in=input_count
            )
            
            # Calculate gross amount
            df_calc = df.withColumn(
                "gross_amount",
                (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
            )
            
            # Calculate discount
            df_calc = df_calc.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_qty_tier2,
                    F.col("gross_amount") * self.discount_rate_tier2
                ).when(
                    F.col("quantity") > self.discount_qty_tier1,
                    F.col("gross_amount") * self.discount_rate_tier1
                ).otherwise(0).cast(DecimalType(16, 2))
            )
            
            # Calculate tax
            df_calc = df_calc.withColumn(
                "tax_amount",
                ((F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate)
                .cast(DecimalType(16, 2))
            )
            
            # Calculate net amount
            df_calc = df_calc.withColumn(
                "net_amount",
                (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate profit margin
            df_calc = df_calc.withColumn(
                "cost_amount",
                (F.col("quantity") * F.col("unit_price") * self.cost_ratio)
                .cast(DecimalType(16, 2))
            )
            
            df_calc = df_calc.withColumn(
                "profit_margin",
                F.when(
                    F.col("net_amount") > 0,
                    ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount") * 100)
                ).otherwise(0).cast(DecimalType(5, 2))
            )
            
            # Categorize sales
            df_calc = df_calc.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold,
                    "HIGH"
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold,
                    "MEDIUM"
                ).otherwise("LOW")
            )
            
            # Generate analytics ID
            df_calc = df_calc.withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL"),
                    F.col("trans_id"),
                    F.date_format(F.current_timestamp(), "HHmmss")
                )
            )
            
            # Add metadata
            df_analytics = df_calc.withColumn(
                "loaded_at",
                F.current_timestamp()
            ).withColumn(
                "loaded_by",
                F.lit("system")
            )
            
            # Select final columns
            df_final = df_analytics.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                F.col("quantity").alias("total_quantity"),
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
                "correlation_id",
                "loaded_at",
                "loaded_by"
            )
            
            output_count = df_final.count()
            
            self.logger.log_transform(
                message=f"Transformed {output_count} of {input_count} records",
                records_in=input_count,
                records_out=output_count,
                success=True,
                business_rules={
                    'discount_tiers': [self.discount_qty_tier1, self.discount_qty_tier2],
                    'tax_rate': self.tax_rate,
                    'category_thresholds': [
                        self.category_medium_threshold,
                        self.category_high_threshold
                    ]
                }
            )
            
            return df_final
            
        except Exception as e:
            self.logger.log_error(
                step='TRANSFORM',
                message=f"Transformation failed: {str(e)}",
                exception=e
            )
            raise