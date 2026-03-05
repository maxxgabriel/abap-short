"""
ETL Transformer - Main transformation logic.
Migrated from ABAP ZCL_ETL_TRANSFORMER class.
"""

from datetime import datetime
from decimal import Decimal
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config_loader import ETLConfig
from src.logger import ETLLogger
from src.schemas import ETLSchemas


class ETLTransformer:
    """
    Transforms raw sales data into analytics format.
    Migrated from ABAP ZCL_ETL_TRANSFORMER.
    
    Replaces ABAP LOOP AT patterns with DataFrame functional transformations.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def transform_data(self, raw_df: DataFrame) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data to analytics format.
        Replaces ABAP transform_data method with DataFrame operations.
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            Tuple of (transformed DataFrame, success flag)
        """
        try:
            self.logger.log_message(
                step=self.config.step_transform,
                status=self.config.status_success,
                message="Starting data transformation"
            )
            
            initial_count = raw_df.count()
            
            # Apply transformations using DataFrame operations
            # This replaces ABAP LOOP AT with functional transformations
            analytics_df = (
                raw_df
                # Calculate gross amount
                .withColumn(
                    "gross_amount",
                    F.col("quantity") * F.col("unit_price")
                )
                # Calculate discount
                .withColumn(
                    "discount_amount",
                    self._calculate_discount_udf(
                        F.col("quantity"),
                        F.col("gross_amount")
                    )
                )
                # Calculate tax
                .withColumn(
                    "tax_amount",
                    (F.col("gross_amount") - F.col("discount_amount")) * 
                    F.lit(self.config.tax_rate)
                )
                # Calculate net amount
                .withColumn(
                    "net_amount",
                    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
                )
                # Calculate profit margin
                .withColumn(
                    "cost_amount",
                    F.col("quantity") * F.col("unit_price") * F.lit(self.config.cost_ratio)
                )
                .withColumn(
                    "profit_margin",
                    F.when(
                        F.col("net_amount") > 0,
                        ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
                    ).otherwise(F.lit(0.0))
                )
                # Categorize sales
                .withColumn(
                    "category",
                    self._categorize_sale_udf(F.col("gross_amount"))
                )
                # Generate analytics ID
                .withColumn(
                    "analytics_id",
                    self._generate_analytics_id_udf(
                        F.col("trans_id"),
                        F.current_timestamp()
                    )
                )
                # Add ETL metadata
                .withColumn("etl_run_id", F.lit(self.logger.get_etl_run_id()))
                .withColumn("loaded_at", F.current_timestamp())
                .withColumn("loaded_by", F.lit("PYSPARK_ETL"))
                # Rename quantity column
                .withColumnRenamed("quantity", "total_quantity")
                # Select final columns matching analytics schema
                .select(
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
                    "loaded_at",
                    "loaded_by"
                )
            )
            
            # Validate transformed data
            final_count = analytics_df.count()
            
            self.logger.log_message(
                step=self.config.step_transform,
                status=self.config.status_success,
                message=f"Transformed {final_count} of {initial_count} records",
                records_processed=initial_count,
                records_success=final_count,
                records_error=initial_count - final_count
            )
            
            return analytics_df, True
            
        except Exception as e:
            self.logger.log_message(
                step=self.config.step_transform,
                status=self.config.status_error,
                message=f"Transformation failed: {str(e)}"
            )
            return self.spark.createDataFrame([], ETLSchemas.analytics_schema()), False
    
    def _calculate_discount_udf(
        self,
        quantity_col: F.Column,
        gross_amount_col: F.Column
    ) -> F.Column:
        """
        Calculate discount based on quantity tiers.
        Replaces ABAP discount calculation logic.
        
        Args:
            quantity_col: Quantity column
            gross_amount_col: Gross amount column
            
        Returns:
            Discount amount column
        """
        return F.when(
            quantity_col > self.config.discount_qty_tier2,
            gross_amount_col * F.lit(self.config.discount_rate_tier2)
        ).when(
            quantity_col > self.config.discount_qty_tier1,
            gross_amount_col * F.lit(self.config.discount_rate_tier1)
        ).otherwise(F.lit(0.0))
    
    def _categorize_sale_udf(self, gross_amount_col: F.Column) -> F.Column:
        """
        Categorize sale based on gross amount.
        Replaces ABAP categorize_sale method.
        
        Args:
            gross_amount_col: Gross amount column
            
        Returns:
            Category column
        """
        return F.when(
            gross_amount_col >= self.config.category_high_threshold,
            F.lit(self.config.category_high)
        ).when(
            gross_amount_col >= self.config.category_medium_threshold,
            F.lit(self.config.category_medium)
        ).otherwise(F.lit(self.config.category_low))
    
    def _generate_analytics_id_udf(
        self,
        trans_id_col: F.Column,
        timestamp_col: F.Column
    ) -> F.Column:
        """
        Generate unique analytics ID.
        Replaces ABAP analytics ID generation logic.
        
        Args:
            trans_id_col: Transaction ID column
            timestamp_col: Timestamp column
            
        Returns:
            Analytics ID column
        """
        return F.concat(
            F.lit(self.config.prefix_analytics_id),
            trans_id_col,
            F.date_format(timestamp_col, "HHmmss")
        )
    
    def calculate_summary_statistics(self, analytics_df: DataFrame) -> DataFrame:
        """
        Calculate summary statistics for transformed data.
        Additional analytical functionality beyond ABAP original.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Summary statistics DataFrame
        """
        summary_df = analytics_df.agg(
            F.count("*").alias("total_records"),
            F.sum("total_quantity").alias("total_quantity_sum"),
            F.sum("gross_amount").alias("total_gross_amount"),
            F.sum("net_amount").alias("total_net_amount"),
            F.sum("discount_amount").alias("total_discount_amount"),
            F.sum("tax_amount").alias("total_tax_amount"),
            F.avg("profit_margin").alias("avg_profit_margin"),
            F.min("trans_date").alias("min_trans_date"),
            F.max("trans_date").alias("max_trans_date")
        )
        
        return summary_df
    
    def get_category_distribution(self, analytics_df: DataFrame) -> DataFrame:
        """
        Get distribution of sales by category.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Category distribution DataFrame
        """
        return (
            analytics_df
            .groupBy("category")
            .agg(
                F.count("*").alias("record_count"),
                F.sum("net_amount").alias("total_net_amount"),
                F.avg("profit_margin").alias("avg_profit_margin")
            )
            .orderBy(F.desc("total_net_amount"))
        )
    
    def get_region_analysis(self, analytics_df: DataFrame) -> DataFrame:
        """
        Get sales analysis by region.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Region analysis DataFrame
        """
        return (
            analytics_df
            .groupBy("region")
            .agg(
                F.count("*").alias("record_count"),
                F.sum("net_amount").alias("total_net_amount"),
                F.avg("net_amount").alias("avg_net_amount"),
                F.sum("total_quantity").alias("total_quantity"),
                F.avg("profit_margin").alias("avg_profit_margin")
            )
            .orderBy(F.desc("total_net_amount"))
        )