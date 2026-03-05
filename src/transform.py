"""
Data Transformation Module
Transforms raw sales data into analytics format with business rules
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from pyspark.sql import functions as F
from typing import Dict, Any

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import TransformError


class SalesDataTransformer:
    """Handles transformation of raw sales data to analytics format"""
    
    ANALYTICS_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            logger: ETL logger
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.statistics: Dict[str, Any] = {}
        
        # Business rules from config
        self.discount_qty_tier1 = config.business_rules['discount_qty_tier1']
        self.discount_qty_tier2 = config.business_rules['discount_qty_tier2']
        self.discount_rate_tier1 = config.business_rules['discount_rate_tier1']
        self.discount_rate_tier2 = config.business_rules['discount_rate_tier2']
        self.tax_rate = config.business_rules['tax_rate']
        self.cost_ratio = config.business_rules['cost_ratio']
        self.category_high = config.business_rules['category_high_threshold']
        self.category_medium = config.business_rules['category_medium_threshold']
    
    def transform_data(self, raw_data: DataFrame) -> DataFrame:
        """
        Transform raw sales data to analytics format
        
        Args:
            raw_data: Raw sales DataFrame
        
        Returns:
            Transformed analytics DataFrame
        
        Raises:
            TransformError: If transformation fails
        """
        try:
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message="Starting data transformation"
            )
            
            # Calculate derived fields
            df = self._calculate_amounts(raw_data)
            df = self._calculate_discounts(df)
            df = self._calculate_tax(df)
            df = self._calculate_net_amount(df)
            df = self._calculate_profit_margin(df)
            df = self._categorize_sales(df)
            df = self._generate_analytics_id(df)
            
            # Add ETL metadata
            df = df.withColumn("etl_run_id", F.lit(self.logger.etl_run_id))
            
            # Select final columns
            df = df.select(
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
                "etl_run_id"
            )
            
            # Validate transformed data
            count = df.count()
            error_count = df.filter(F.col("net_amount").isNull()).count()
            
            self.statistics['records_transformed'] = count
            self.statistics['records_failed'] = error_count
            
            self.logger.log_message(
                step='TRANSFORM',
                status='S',
                message=f"Transformed {count} records successfully",
                records_processed=count,
                records_success=count - error_count,
                records_error=error_count
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='TRANSFORM',
                status='E',
                message=f"Transformation failed: {str(e)}"
            )
            raise TransformError(f"Data transformation failed: {str(e)}", original_error=e)
    
    def _calculate_amounts(self, df: DataFrame) -> DataFrame:
        """Calculate gross amounts"""
        return df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
    
    def _calculate_discounts(self, df: DataFrame) -> DataFrame:
        """Calculate discount amounts based on quantity tiers"""
        return df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.discount_qty_tier2,
                F.col("gross_amount") * self.discount_rate_tier2
            ).when(
                F.col("quantity") > self.discount_qty_tier1,
                F.col("gross_amount") * self.discount_rate_tier1
            ).otherwise(F.lit(0.0))
        )
    
    def _calculate_tax(self, df: DataFrame) -> DataFrame:
        """Calculate tax on discounted amount"""
        return df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate
        )
    
    def _calculate_net_amount(self, df: DataFrame) -> DataFrame:
        """Calculate net amount (gross - discount + tax)"""
        return df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """Calculate profit margin percentage"""
        return df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * self.cost_ratio
        ).withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0.0))
        ).drop("cost_amount")
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """Categorize sales based on gross amount"""
        return df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= self.category_high,
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= self.category_medium,
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )
    
    def _generate_analytics_id(self, df: DataFrame) -> DataFrame:
        """Generate unique analytics ID"""
        return df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
    
    def validate_prerequisites(self) -> bool:
        """
        Validate transformation prerequisites
        
        Returns:
            True if prerequisites are met
        """
        # Validate business rules are configured
        required_rules = [
            'discount_qty_tier1',
            'discount_rate_tier1',
            'tax_rate',
            'cost_ratio'
        ]
        
        return all(
            rule in self.config.business_rules
            for rule in required_rules
        )