"""
Data transformation module for ETL system.
Converted from ABAP ZCL_ETL_TRANSFORMER class.
"""
from typing import Optional, Tuple
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, concat, current_timestamp,
    date_format, expr, udf
)
from pyspark.sql.types import StringType, DecimalType

from src.schemas import ETLSchemas
from src.constants import ETLConstants
from src.logger import ETLLogger


class ETLTransformer:
    """
    Transforms raw sales data into analytics format.
    
    Converted from ABAP ZCL_ETL_TRANSFORMER.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.schema = ETLSchemas.analytics_schema()
    
    def transform_data(
        self,
        raw_df: DataFrame
    ) -> Tuple[Optional[DataFrame], bool]:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            Tuple of (transformed DataFrame or None, success flag)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.SUCCESS,
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Apply transformations
            df_transformed = self._calculate_analytics(raw_df)
            
            # Validate transformed data
            df_validated = self._validate_records(df_transformed)
            
            output_count = df_validated.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.SUCCESS,
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return df_validated, True
            
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.TRANSFORM,
                status=ETLConstants.Status.ERROR,
                message=f"Transformation failed: {str(e)}"
            )
            return None, False
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Calculate analytics fields from raw data.
        
        Args:
            df: Raw sales DataFrame
            
        Returns:
            DataFrame with calculated analytics fields
        """
        # Get business rules from config
        business_rules = self.config.get("business_rules", {})
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            (col("quantity") * col("unit_price")).cast(DecimalType(16, 2))
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(
                col("quantity") > ETLConstants.DISCOUNT_QTY_TIER2,
                col("gross_amount") * lit(ETLConstants.DISCOUNT_RATE_TIER2)
            ).when(
                col("quantity") > ETLConstants.DISCOUNT_QTY_TIER1,
                col("gross_amount") * lit(ETLConstants.DISCOUNT_RATE_TIER1)
            ).otherwise(
                lit(0)
            ).cast(DecimalType(16, 2))
        )
        
        # Calculate tax
        df = df.withColumn(
            "tax_amount",
            ((col("gross_amount") - col("discount_amount")) * 
             lit(ETLConstants.TAX_RATE)).cast(DecimalType(16, 2))
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            (col("gross_amount") - col("discount_amount") + 
             col("tax_amount")).cast(DecimalType(16, 2))
        )
        
        # Calculate cost and profit margin
        df = df.withColumn(
            "cost",
            (col("quantity") * col("unit_price") * 
             lit(ETLConstants.COST_RATIO)).cast(DecimalType(16, 2))
        )
        
        df = df.withColumn(
            "profit_margin",
            when(
                col("net_amount") > 0,
                (((col("net_amount") - col("cost")) / col("net_amount")) * 
                 lit(100)).cast(DecimalType(5, 2))
            ).otherwise(lit(0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            self._categorize_sale_udf()(col("gross_amount"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            concat(
                lit(ETLConstants.PREFIX_ANALYTICS_ID),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.logger.get_etl_run_id()))
        df = df.withColumn("loaded_at", current_timestamp())
        df = df.withColumn("loaded_by", lit("PYSPARK_ETL"))
        
        # Select final columns matching analytics schema
        return df.select(
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
    
    def _categorize_sale_udf(self):
        """
        Create UDF for categorizing sales.
        
        Returns:
            UDF function
        """
        def categorize(amount):
            if amount is None:
                return ETLConstants.Category.LOW
            
            amount_decimal = Decimal(str(amount))
            
            if amount_decimal >= ETLConstants.CATEGORY_HIGH_THRESHOLD:
                return ETLConstants.Category.HIGH
            elif amount_decimal >= ETLConstants.CATEGORY_MEDIUM_THRESHOLD:
                return ETLConstants.Category.MEDIUM
            else:
                return ETLConstants.Category.LOW
        
        return udf(categorize, StringType())
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed records.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        # Remove records with null required fields
        df_valid = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0)
        )
        
        # Validate currency
        df_valid = df_valid.filter(col("currency").isNotNull())
        
        # Validate category
        valid_categories = [
            ETLConstants.Category.HIGH,
            ETLConstants.Category.MEDIUM,
            ETLConstants.Category.LOW
        ]
        df_valid = df_valid.filter(col("category").isin(valid_categories))
        
        return df_valid