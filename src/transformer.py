"""
Data transformation component.
"""
from decimal import Decimal
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, udf, current_timestamp
)
from pyspark.sql.types import StringType, DecimalType
from src.logger import ETLLogger
from src.constants import ETLConstants
from src.exceptions import TransformationError


class ETLTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, logger: ETLLogger, spark: SparkSession, config: dict):
        self.logger = logger
        self.spark = spark
        self.config = config
    
    def transform_data(self, raw_data: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_data: DataFrame with raw sales data
            
        Returns:
            DataFrame with transformed analytics data
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                ETLConstants.STEPS.TRANSFORM,
                ETLConstants.STATUS.INFO,
                "Starting data transformation"
            )
            
            input_count = raw_data.count()
            
            # Calculate gross amount
            df = raw_data.withColumn(
                "gross_amount",
                (col("quantity") * col("unit_price")).cast(DecimalType(16, 2))
            )
            
            # Calculate discount
            df = self._apply_discount(df)
            
            # Calculate tax
            df = self._apply_tax(df)
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                (col("gross_amount") - col("discount_amount") + col("tax_amount"))
                .cast(DecimalType(16, 2))
            )
            
            # Calculate profit margin
            df = self._calculate_profit_margin(df)
            
            # Categorize sales
            df = self._categorize_sales(df)
            
            # Generate analytics ID
            analytics_id_udf = udf(self._generate_analytics_id, StringType())
            df = df.withColumn(
                "analytics_id",
                analytics_id_udf(col("trans_id"))
            )
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", lit(self.logger.etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            df = df.withColumn("loaded_by", lit("ETL_SYSTEM"))
            
            # Select final columns
            analytics_df = df.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                col("quantity").alias("total_quantity"),
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
            
            output_count = analytics_df.count()
            
            self.logger.log_message(
                ETLConstants.STEPS.TRANSFORM,
                ETLConstants.STATUS.SUCCESS,
                f"Transformation completed successfully",
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                ETLConstants.STEPS.TRANSFORM,
                ETLConstants.STATUS.ERROR,
                f"Transformation failed: {str(e)}"
            )
            raise TransformationError(
                f"Failed to transform data: {str(e)}",
                step=ETLConstants.STEPS.TRANSFORM
            )
    
    def _apply_discount(self, df: DataFrame) -> DataFrame:
        """Apply discount based on quantity tiers."""
        discount_config = self.config.get("etl", {}).get("discount", {})
        tier1_qty = discount_config.get("tier1_quantity", ETLConstants.DISCOUNT_QTY_TIER1)
        tier2_qty = discount_config.get("tier2_quantity", ETLConstants.DISCOUNT_QTY_TIER2)
        tier1_rate = discount_config.get("tier1_rate", ETLConstants.DISCOUNT_RATE_TIER1)
        tier2_rate = discount_config.get("tier2_rate", ETLConstants.DISCOUNT_RATE_TIER2)
        
        return df.withColumn(
            "discount_amount",
            when(col("quantity") > tier2_qty, col("gross_amount") * lit(tier2_rate))
            .when(col("quantity") > tier1_qty, col("gross_amount") * lit(tier1_rate))
            .otherwise(lit(0))
            .cast(DecimalType(16, 2))
        )
    
    def _apply_tax(self, df: DataFrame) -> DataFrame:
        """Apply tax on gross amount minus discount."""
        tax_rate = self.config.get("etl", {}).get("tax_rate", ETLConstants.TAX_RATE)
        
        return df.withColumn(
            "tax_amount",
            ((col("gross_amount") - col("discount_amount")) * lit(tax_rate))
            .cast(DecimalType(16, 2))
        )
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """Calculate profit margin percentage."""
        cost_ratio = self.config.get("etl", {}).get("cost_ratio", ETLConstants.COST_RATIO)
        
        df = df.withColumn(
            "cost_amount",
            (col("quantity") * col("unit_price") * lit(cost_ratio))
            .cast(DecimalType(16, 2))
        )
        
        return df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 ((col("net_amount") - col("cost_amount")) / col("net_amount") * 100)
            ).otherwise(lit(0))
            .cast(DecimalType(5, 2))
        ).drop("cost_amount")
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """Categorize sales based on gross amount."""
        category_config = self.config.get("etl", {}).get("category", {})
        high_threshold = category_config.get(
            "high_threshold",
            ETLConstants.CATEGORY_HIGH_THRESHOLD
        )
        medium_threshold = category_config.get(
            "medium_threshold",
            ETLConstants.CATEGORY_MEDIUM_THRESHOLD
        )
        
        return df.withColumn(
            "category",
            when(col("gross_amount") >= high_threshold, lit(ETLConstants.CATEGORIES.HIGH))
            .when(col("gross_amount") >= medium_threshold, lit(ETLConstants.CATEGORIES.MEDIUM))
            .otherwise(lit(ETLConstants.CATEGORIES.LOW))
        )
    
    @staticmethod
    def _generate_analytics_id(trans_id: str) -> str:
        """Generate unique analytics ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{ETLConstants.PREFIX_ANALYTICS_ID}{trans_id}{timestamp[-6:]}"