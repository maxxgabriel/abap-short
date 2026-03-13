"""
Transform module for Sales ETL Pipeline
Transforms raw sales data into analytics format with business logic
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, concat, current_timestamp,
    date_format, expr, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
import logging


class SalesTransformer:
    """Handles transformation of raw sales data into analytics format"""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger, etl_run_id: str):
        """
        Initialize the transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.etl_run_id = etl_run_id
        self.business_rules = config['transform']['business_rules']
    
    def get_analytics_schema(self) -> StructType:
        """
        Define the schema for analytics data
        
        Returns:
            StructType: Schema definition for analytics data
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
    
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: Raw sales DataFrame
            
        Returns:
            DataFrame: Transformed analytics data
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            
            # Calculate discount based on quantity
            df = self._apply_discount_logic(df)
            
            # Calculate tax
            df = self._calculate_tax(df)
            
            # Calculate net amount
            df = df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Calculate profit margin
            df = self._calculate_profit_margin(df)
            
            # Categorize sales
            df = self._categorize_sales(df)
            
            # Generate analytics ID
            df = self._generate_analytics_id(df)
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", lit(self.etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            
            # Rename columns to match analytics schema
            df = df.withColumnRenamed("quantity", "total_quantity")
            
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
            
            record_count = analytics_df.count()
            self.logger.info(f"Transformed {record_count} records successfully")
            
            return analytics_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_discount_logic(self, df: DataFrame) -> DataFrame:
        """
        Apply discount rules based on quantity tiers
        
        Args:
            df: DataFrame with quantity and gross_amount
            
        Returns:
            DataFrame: DataFrame with discount_amount column
        """
        tier1_qty = self.business_rules['discount']['quantity_tier1']
        tier2_qty = self.business_rules['discount']['quantity_tier2']
        tier1_rate = self.business_rules['discount']['rate_tier1']
        tier2_rate = self.business_rules['discount']['rate_tier2']
        
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > tier2_qty, col("gross_amount") * tier2_rate)
            .when(col("quantity") > tier1_qty, col("gross_amount") * tier1_rate)
            .otherwise(0)
        )
        
        return df
    
    def _calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax on discounted amount
        
        Args:
            df: DataFrame with gross_amount and discount_amount
            
        Returns:
            DataFrame: DataFrame with tax_amount column
        """
        tax_rate = self.business_rules['tax']['rate']
        
        df = df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * tax_rate
        )
        
        return df
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin based on cost ratio
        
        Args:
            df: DataFrame with pricing information
            
        Returns:
            DataFrame: DataFrame with profit_margin column
        """
        cost_ratio = self.business_rules['profit']['cost_ratio']
        
        df = df.withColumn(
            "cost_amount",
            col("quantity") * col("unit_price") * cost_ratio
        )
        
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2)
            ).otherwise(0)
        )
        
        return df.drop("cost_amount")
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount thresholds
        
        Args:
            df: DataFrame with gross_amount
            
        Returns:
            DataFrame: DataFrame with category column
        """
        high_threshold = self.business_rules['category']['high_threshold']
        medium_threshold = self.business_rules['category']['medium_threshold']
        
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= high_threshold, "HIGH")
            .when(col("gross_amount") >= medium_threshold, "MEDIUM")
            .otherwise("LOW")
        )
        
        return df
    
    def _generate_analytics_id(self, df: DataFrame) -> DataFrame:
        """
        Generate unique analytics ID for each record
        
        Args:
            df: DataFrame with trans_id
            
        Returns:
            DataFrame: DataFrame with analytics_id column
        """
        df = df.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                date_format(current_timestamp(), "HHmmss")
            )
        )
        
        return df
    
    def validate_transformed_data(self, df: DataFrame) -> bool:
        """
        Validate transformed data quality
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            bool: True if validation passes
        """
        try:
            # Check for negative amounts
            invalid_amounts = df.filter(
                (col("gross_amount") < 0) |
                (col("net_amount") < 0)
            ).count()
            
            if invalid_amounts > 0:
                self.logger.error(f"Found {invalid_amounts} records with negative amounts")
                return False
            
            # Check category values
            valid_categories = ["HIGH", "MEDIUM", "LOW"]
            invalid_categories = df.filter(
                ~col("category").isin(valid_categories)
            ).count()
            
            if invalid_categories > 0:
                self.logger.error(f"Found {invalid_categories} records with invalid categories")
                return False
            
            # Check for null analytics_id
            null_ids = df.filter(col("analytics_id").isNull()).count()
            if null_ids > 0:
                self.logger.error(f"Found {null_ids} records with null analytics_id")
                return False
            
            self.logger.info("Transformed data validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Transformed data validation failed: {str(e)}")
            return False