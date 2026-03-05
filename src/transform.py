"""
Data Transformation Module
Transforms raw sales data into analytics format
"""
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.utils.logger import ETLLogger


class DataTransformer:
    """Handles data transformation logic"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        logger: ETLLogger,
        etl_run_id: str
    ):
        """
        Initialize data transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
            etl_run_id: ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.etl_run_id = etl_run_id
        
        # Load business rules from config
        business_rules = config.get('business_rules', {})
        self.discount_qty_tier1 = business_rules.get('discount_qty_tier1', 10)
        self.discount_qty_tier2 = business_rules.get('discount_qty_tier2', 15)
        self.discount_rate_tier1 = business_rules.get('discount_rate_tier1', 0.05)
        self.discount_rate_tier2 = business_rules.get('discount_rate_tier2', 0.10)
        self.tax_rate = business_rules.get('tax_rate', 0.08)
        self.cost_ratio = business_rules.get('cost_ratio', 0.60)
        self.category_high_threshold = business_rules.get('category_high_threshold', 2000.00)
        self.category_medium_threshold = business_rules.get('category_medium_threshold', 500.00)
        
        # Define schema for analytics data
        self.analytics_schema = StructType([
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
            StructField("etl_run_id", StringType(), False),
        ])
    
    def transform_data(self, raw_data: DataFrame) -> Optional[DataFrame]:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_data: DataFrame containing raw sales data
            
        Returns:
            DataFrame containing transformed analytics data
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message='Starting data transformation'
            )
            
            initial_count = raw_data.count()
            
            # Calculate analytics fields
            df = self._calculate_analytics(raw_data)
            
            # Validate transformed data
            df = self._validate_records(df)
            
            final_count = df.count()
            error_count = initial_count - final_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                records_processed=initial_count,
                records_success=final_count,
                records_error=error_count,
                message=f'Transformed {final_count} of {initial_count} records'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f'Transformation failed: {str(e)}'
            )
            raise
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """Apply business logic transformations"""
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(F.col("quantity") > self.discount_qty_tier2,
                   F.col("gross_amount") * F.lit(self.discount_rate_tier2))
            .when(F.col("quantity") > self.discount_qty_tier1,
                  F.col("gross_amount") * F.lit(self.discount_rate_tier1))
            .otherwise(F.lit(0.0))
        )
        
        # Calculate tax on (gross - discount)
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * F.lit(self.tax_rate)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        # Cost = quantity * unit_price * cost_ratio
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(self.cost_ratio)
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(F.col("net_amount") > 0,
                   ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
            .otherwise(F.lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            F.when(F.col("gross_amount") >= self.category_high_threshold, F.lit("HIGH"))
            .when(F.col("gross_amount") >= self.category_medium_threshold, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL run ID
        df = df.withColumn("etl_run_id", F.lit(self.etl_run_id))
        
        # Rename quantity column
        df = df.withColumn("total_quantity", F.col("quantity"))
        
        # Select final columns
        return df.select(
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
            "etl_run_id"
        )
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """Validate transformed records"""
        
        # Filter out invalid records
        valid_df = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return valid_df