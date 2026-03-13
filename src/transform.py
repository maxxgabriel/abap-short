"""
Transform module for Sales ETL process.
Transforms raw sales data into analytics format with business logic.
"""
from decimal import Decimal
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.logger import ETLLogger
from src.schemas import SALES_ANALYTICS_SCHEMA


class SalesTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Business rules from config
        self.discount_qty_tier1 = config['business_rules']['discount_qty_tier1']
        self.discount_qty_tier2 = config['business_rules']['discount_qty_tier2']
        self.discount_rate_tier1 = Decimal(str(config['business_rules']['discount_rate_tier1']))
        self.discount_rate_tier2 = Decimal(str(config['business_rules']['discount_rate_tier2']))
        self.tax_rate = Decimal(str(config['business_rules']['tax_rate']))
        self.cost_ratio = Decimal(str(config['business_rules']['cost_ratio']))
        self.category_high_threshold = Decimal(str(config['business_rules']['category_high_threshold']))
        self.category_medium_threshold = Decimal(str(config['business_rules']['category_medium_threshold']))
    
    def transform_data(
        self, 
        raw_df: DataFrame,
        etl_run_id: str
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Tuple of (analytics DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            # Calculate analytics fields
            transformed_df = self._apply_transformations(raw_df, etl_run_id)
            
            # Validate transformed data
            valid_df, invalid_count = self._validate_records(transformed_df)
            
            success_count = valid_df.count()
            total_count = raw_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=invalid_count,
                message=f"Transformed {success_count} of {total_count} records"
            )
            
            return valid_df, True
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            return self.spark.createDataFrame([], SALES_ANALYTICS_SCHEMA), False
    
    def _apply_transformations(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Apply business logic transformations.
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame
        """
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.discount_qty_tier2,
                F.col("gross_amount") * float(self.discount_rate_tier2)
            ).when(
                F.col("quantity") > self.discount_qty_tier1,
                F.col("gross_amount") * float(self.discount_rate_tier1)
            ).otherwise(0).cast(DecimalType(16, 2))
        )
        
        # Calculate tax amount (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            ((F.col("gross_amount") - F.col("discount_amount")) * float(self.tax_rate))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
            .cast(DecimalType(16, 2))
        )
        
        # Calculate profit margin
        # Profit = (Net Amount - Cost) / Net Amount * 100
        # Cost = Quantity * Unit Price * Cost Ratio
        df = df.withColumn(
            "cost_amount",
            (F.col("quantity") * F.col("unit_price") * float(self.cost_ratio))
            .cast(DecimalType(16, 2))
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                (((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
            ).otherwise(0).cast(DecimalType(5, 2))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= float(self.category_high_threshold),
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= float(self.category_medium_threshold),
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", F.lit(etl_run_id))
        df = df.withColumn("loaded_at", F.current_timestamp())
        df = df.withColumn("loaded_by", F.lit("SPARK_ETL"))
        
        # Rename quantity field
        df = df.withColumnRenamed("quantity", "total_quantity")
        
        # Select final columns in correct order
        df = df.select(
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
        
        return df
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """
        Validate transformed records.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (valid DataFrame, invalid count)
        """
        # Define validation conditions
        valid_conditions = (
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        # Split into valid and invalid
        valid_df = df.filter(valid_conditions)
        invalid_df = df.filter(~valid_conditions)
        
        invalid_count = invalid_df.count()
        
        if invalid_count > 0:
            self.logger.log_message(
                step="TRANSFORM",
                status="W",
                message=f"Found {invalid_count} invalid records"
            )
        
        return valid_df, invalid_count