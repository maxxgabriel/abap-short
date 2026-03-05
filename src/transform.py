"""
Transform module for Sales ETL System.
Transforms raw sales data into analytics format with business logic.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, round as spark_round, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Dict
import logging


class SalesTransformer:
    """Transforms raw sales data into analytics format."""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger, config: Dict):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            logger: Logger instance
            config: Configuration dictionary with business rules
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self._schema = self._get_analytics_schema()
    
    def _get_analytics_schema(self) -> StructType:
        """Define the schema for analytics data."""
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
            StructField("etl_run_id", StringType(), nullable=False)
        ])
    
    def transform_data(self, df_raw: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: ETL run identifier
        
        Returns:
            Transformed analytics DataFrame
        
        Raises:
            Exception: If transformation fails
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Get business rules from config
            discount_qty_tier1 = self.config['business_rules']['discount_qty_tier1']
            discount_qty_tier2 = self.config['business_rules']['discount_qty_tier2']
            discount_rate_tier1 = self.config['business_rules']['discount_rate_tier1']
            discount_rate_tier2 = self.config['business_rules']['discount_rate_tier2']
            tax_rate = self.config['business_rules']['tax_rate']
            cost_ratio = self.config['business_rules']['cost_ratio']
            category_high = self.config['business_rules']['category_high_threshold']
            category_medium = self.config['business_rules']['category_medium_threshold']
            
            # Calculate derived fields
            df_transformed = df_raw.select(
                # Generate analytics ID
                concat(lit("ANL"), col("trans_id")).alias("analytics_id"),
                col("trans_date"),
                col("customer_id"),
                col("product_id"),
                col("quantity").alias("total_quantity"),
                
                # Calculate gross amount
                (col("quantity") * col("unit_price")).alias("gross_amount"),
                
                col("currency"),
                col("sales_rep"),
                col("region")
            )
            
            # Calculate discount amount based on quantity tiers
            df_transformed = df_transformed.withColumn(
                "discount_amount",
                when(col("total_quantity") > discount_qty_tier2, 
                     col("gross_amount") * discount_rate_tier2)
                .when(col("total_quantity") > discount_qty_tier1, 
                      col("gross_amount") * discount_rate_tier1)
                .otherwise(lit(0.0))
            )
            
            # Calculate tax amount (on gross - discount)
            df_transformed = df_transformed.withColumn(
                "tax_amount",
                (col("gross_amount") - col("discount_amount")) * tax_rate
            )
            
            # Calculate net amount
            df_transformed = df_transformed.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Calculate profit margin
            # Simplified: assume cost is cost_ratio% of gross amount
            df_transformed = df_transformed.withColumn(
                "cost_estimate",
                col("gross_amount") * cost_ratio
            )
            
            df_transformed = df_transformed.withColumn(
                "profit_margin",
                when(col("net_amount") > 0,
                     spark_round(((col("net_amount") - col("cost_estimate")) / col("net_amount")) * 100, 2))
                .otherwise(lit(0.0))
            ).drop("cost_estimate")
            
            # Categorize sales
            df_transformed = df_transformed.withColumn(
                "category",
                when(col("gross_amount") >= category_high, lit("HIGH"))
                .when(col("gross_amount") >= category_medium, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            
            # Add ETL run ID
            df_transformed = df_transformed.withColumn("etl_run_id", lit(etl_run_id))
            
            # Round decimal fields
            for field_name in ["gross_amount", "net_amount", "discount_amount", "tax_amount"]:
                df_transformed = df_transformed.withColumn(
                    field_name,
                    spark_round(col(field_name), 2)
                )
            
            record_count = df_transformed.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def categorize_sale(self, gross_amount: float) -> str:
        """
        Categorize sale based on gross amount.
        
        Args:
            gross_amount: Gross sale amount
        
        Returns:
            Category string (HIGH, MEDIUM, LOW)
        """
        category_high = self.config['business_rules']['category_high_threshold']
        category_medium = self.config['business_rules']['category_medium_threshold']
        
        if gross_amount >= category_high:
            return "HIGH"
        elif gross_amount >= category_medium:
            return "MEDIUM"
        else:
            return "LOW"


class TransformerInterface:
    """Interface contract for all transformer components."""
    
    def transform_data(self, df: DataFrame, **kwargs) -> DataFrame:
        """Transform input DataFrame."""
        raise NotImplementedError("Subclasses must implement transform_data()")
    
    def get_component_name(self) -> str:
        """Return component name."""
        raise NotImplementedError("Subclasses must implement get_component_name()")
    
    def validate_schema(self, df: DataFrame) -> bool:
        """Validate input DataFrame schema."""
        raise NotImplementedError("Subclasses must implement validate_schema()")