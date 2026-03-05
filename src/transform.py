"""
ETL Transformer Module
Transforms raw sales data into analytics format using PySpark.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime

from src.utils.etl_logger import ETLLogger


class ETLTransformer:
    """
    Transforms raw sales data into analytics format.
    Converted from ZCL_ETL_TRANSFORMER ABAP class.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.analytics_schema = self._get_analytics_schema()
    
    def _get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data.
        
        Returns:
            StructType schema definition
        """
        return StructType([
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
            StructField("etl_run_id", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame with raw sales data
            
        Returns:
            DataFrame with transformed analytics data
            
        Raises:
            Exception: If transformation fails
        """
        step = self.config['process_steps']['transform']
        
        try:
            self.logger.log_etl_message(
                step=step,
                status=self.config['status_codes']['success'],
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Calculate business metrics
            transformed_df = self._calculate_analytics(raw_df)
            
            output_count = transformed_df.count()
            
            self.logger.log_etl_statistics(
                step=step,
                status=self.config['status_codes']['success'],
                records_processed=input_count,
                records_success=output_count,
                records_error=input_count - output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.log_etl_message(
                step=step,
                status=self.config['status_codes']['error'],
                message=f"Transformation failed: {str(e)}"
            )
            raise
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Calculate analytics metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame with calculated metrics
        """
        # Get configuration values
        discount_tier1_qty = self.config['discount']['quantity_tier1']
        discount_tier2_qty = self.config['discount']['quantity_tier2']
        discount_tier1_rate = self.config['discount']['rate_tier1']
        discount_tier2_rate = self.config['discount']['rate_tier2']
        tax_rate = self.config['tax']['rate']
        cost_ratio = self.config['cost']['ratio']
        category_high = self.config['category_thresholds']['high']
        category_medium = self.config['category_thresholds']['medium']
        
        # Calculate gross amount
        df = df.withColumn("gross_amount", col("quantity") * col("unit_price"))
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_tier2_qty, col("gross_amount") * discount_tier2_rate)
            .when(col("quantity") > discount_tier1_qty, col("gross_amount") * discount_tier1_rate)
            .otherwise(lit(0.0))
        )
        
        # Calculate tax amount (on gross - discount)
        df = df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * tax_rate
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            col("gross_amount") - col("discount_amount") + col("tax_amount")
        )
        
        # Calculate profit margin (simplified: cost is % of unit price)
        df = df.withColumn("cost_amount", col("quantity") * col("unit_price") * cost_ratio)
        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0, 
                 ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100)
            .otherwise(lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= category_high, lit(self.config['categories']['high']))
            .when(col("gross_amount") >= category_medium, lit(self.config['categories']['medium']))
            .otherwise(lit(self.config['categories']['low']))
        )
        
        # Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            lit(self.config['id_prefixes']['analytics']) + col("trans_id")
        )
        
        # Add ETL run ID
        df = df.withColumn("etl_run_id", lit(self.logger.get_etl_run_id()))
        
        # Select and rename final columns
        result_df = df.select(
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
            "etl_run_id"
        )
        
        return result_df