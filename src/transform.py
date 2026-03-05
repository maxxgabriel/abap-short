"""
Data Transformation Module
Transforms raw sales data into analytics format with business logic
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, round as spark_round, current_timestamp,
    monotonically_increasing_id, concat, lpad, date_format
)
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType
from decimal import Decimal

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import TransformException


class DataTransformer:
    """Transform raw sales data with business rules"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize data transformer
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def _get_schema(self) -> StructType:
        """
        Define schema for analytics data
        
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
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def _calculate_discount(self, df: DataFrame) -> DataFrame:
        """
        Calculate discount based on quantity tiers
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with discount_amount column
        """
        discount_qty_tier1 = self.config.business_rules['discount_qty_tier1']
        discount_qty_tier2 = self.config.business_rules['discount_qty_tier2']
        discount_rate_tier1 = Decimal(str(self.config.business_rules['discount_rate_tier1']))
        discount_rate_tier2 = Decimal(str(self.config.business_rules['discount_rate_tier2']))
        
        return df.withColumn(
            "discount_amount",
            when(col("quantity") > discount_qty_tier2, col("gross_amount") * discount_rate_tier2)
            .when(col("quantity") > discount_qty_tier1, col("gross_amount") * discount_rate_tier1)
            .otherwise(lit(0))
        )
    
    def _calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax on net amount after discount
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with tax_amount column
        """
        tax_rate = Decimal(str(self.config.business_rules['tax_rate']))
        
        return df.withColumn(
            "tax_amount",
            (col("gross_amount") - col("discount_amount")) * tax_rate
        )
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin based on cost ratio
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with profit_margin column
        """
        cost_ratio = Decimal(str(self.config.business_rules['cost_ratio']))
        
        # Calculate cost and profit margin
        df = df.withColumn("cost_amount", col("total_quantity") * col("unit_price") * cost_ratio)
        
        df = df.withColumn(
            "profit_margin",
            spark_round(
                ((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100,
                2
            )
        )
        
        return df.drop("cost_amount")
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category column
        """
        high_threshold = Decimal(str(self.config.business_rules['category_high_threshold']))
        medium_threshold = Decimal(str(self.config.business_rules['category_medium_threshold']))
        
        return df.withColumn(
            "category",
            when(col("gross_amount") >= high_threshold, lit("HIGH"))
            .when(col("gross_amount") >= medium_threshold, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
    
    def _generate_analytics_id(self, df: DataFrame) -> DataFrame:
        """
        Generate unique analytics ID for each record
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with analytics_id column
        """
        # Generate unique ID using transaction ID and timestamp
        df = df.withColumn("row_num", monotonically_increasing_id())
        
        df = df.withColumn(
            "analytics_id",
            concat(
                lit("ANL"),
                col("trans_id"),
                lpad(col("row_num").cast("string"), 6, "0")
            )
        )
        
        return df.drop("row_num")
    
    def transform_data(self, raw_df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Apply complete transformation pipeline to raw data
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: Current ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
            
        Raises:
            TransformException: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            # Step 1: Calculate gross amount
            df = raw_df.withColumn(
                "gross_amount",
                col("quantity") * col("unit_price")
            )
            
            # Step 2: Calculate discount
            df = self._calculate_discount(df)
            
            # Step 3: Calculate tax
            df = self._calculate_tax(df)
            
            # Step 4: Calculate net amount
            df = df.withColumn(
                "net_amount",
                col("gross_amount") - col("discount_amount") + col("tax_amount")
            )
            
            # Step 5: Calculate profit margin
            df = self._calculate_profit_margin(df)
            
            # Step 6: Categorize sales
            df = self._categorize_sales(df)
            
            # Step 7: Generate analytics ID
            df = self._generate_analytics_id(df)
            
            # Step 8: Add ETL metadata
            df = df.withColumn("etl_run_id", lit(etl_run_id))
            df = df.withColumn("loaded_at", current_timestamp())
            
            # Step 9: Round decimal values
            df = df.withColumn("gross_amount", spark_round(col("gross_amount"), 2))
            df = df.withColumn("net_amount", spark_round(col("net_amount"), 2))
            df = df.withColumn("discount_amount", spark_round(col("discount_amount"), 2))
            df = df.withColumn("tax_amount", spark_round(col("tax_amount"), 2))
            
            # Step 10: Select and rename columns for analytics
            analytics_df = df.select(
                col("analytics_id"),
                col("trans_date"),
                col("customer_id"),
                col("product_id"),
                col("quantity").alias("total_quantity"),
                col("gross_amount"),
                col("net_amount"),
                col("discount_amount"),
                col("tax_amount"),
                col("currency"),
                col("sales_rep"),
                col("region"),
                col("profit_margin"),
                col("category"),
                col("etl_run_id"),
                col("loaded_at")
            )
            
            # Count records
            record_count = analytics_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Transformed {record_count} records successfully"
            )
            
            return analytics_df
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=error_msg
            )
            raise TransformException(error_msg)
    
    def validate_prerequisites(self) -> bool:
        """
        Validate transformer prerequisites
        
        Returns:
            True if prerequisites are met
        """
        try:
            # Check if Spark session is active
            if self.spark is None:
                return False
            
            # Check configuration
            if self.config is None:
                return False
            
            # Check business rules
            required_rules = [
                'discount_qty_tier1', 'discount_qty_tier2',
                'discount_rate_tier1', 'discount_rate_tier2',
                'tax_rate', 'cost_ratio',
                'category_high_threshold', 'category_medium_threshold'
            ]
            
            for rule in required_rules:
                if rule not in self.config.business_rules:
                    return False
            
            return True
            
        except Exception:
            return False