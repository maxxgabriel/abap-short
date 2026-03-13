"""
Data Transformation Module for Sales ETL
Transforms raw sales data into analytics format with business rules
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from decimal import Decimal
from typing import Dict, Any
import yaml
import logging


class SalesTransformer:
    """Transforms raw sales data into analytics format"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize transformer with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.etl_run_id = None
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.warning(f"Could not load config: {e}, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            'business_rules': {
                'discount': {
                    'qty_tier1': 10,
                    'qty_tier2': 15,
                    'rate_tier1': Decimal('0.05'),
                    'rate_tier2': Decimal('0.10')
                },
                'tax_rate': Decimal('0.08'),
                'cost_ratio': Decimal('0.60'),
                'category': {
                    'high_threshold': Decimal('2000.00'),
                    'medium_threshold': Decimal('500.00')
                }
            }
        }
    
    def get_analytics_schema(self) -> StructType:
        """Define schema for analytics data"""
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format
        
        Args:
            raw_df: DataFrame with raw sales data
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with transformed analytics data
        """
        self.etl_run_id = etl_run_id
        self.logger.info(f"Starting transformation for ETL run: {etl_run_id}")
        
        # Calculate gross amount
        transformed_df = raw_df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        rules = self.config['business_rules']
        transformed_df = transformed_df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > rules['discount']['qty_tier2'],
                F.col("gross_amount") * F.lit(float(rules['discount']['rate_tier2']))
            ).when(
                F.col("quantity") > rules['discount']['qty_tier1'],
                F.col("gross_amount") * F.lit(float(rules['discount']['rate_tier1']))
            ).otherwise(F.lit(0.0))
        )
        
        # Calculate tax on discounted amount
        transformed_df = transformed_df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * F.lit(float(rules['tax_rate']))
        )
        
        # Calculate net amount
        transformed_df = transformed_df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        transformed_df = transformed_df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(float(rules['cost_ratio']))
        )
        
        transformed_df = transformed_df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0.0))
        )
        
        # Categorize sales
        cat_rules = rules['category']
        transformed_df = transformed_df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= float(cat_rules['high_threshold']),
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= float(cat_rules['medium_threshold']),
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )
        
        # Generate analytics ID
        transformed_df = transformed_df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Add ETL metadata
        transformed_df = transformed_df.withColumn(
            "etl_run_id", F.lit(etl_run_id)
        ).withColumn(
            "loaded_at", F.current_timestamp()
        )
        
        # Select and rename columns for final schema
        analytics_df = transformed_df.select(
            "analytics_id",
            F.col("trans_date"),
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
            "etl_run_id",
            "loaded_at"
        )
        
        # Log transformation statistics
        record_count = analytics_df.count()
        self.logger.info(f"Transformation complete: {record_count} records processed")
        
        return analytics_df
    
    def validate_transformed_data(self, analytics_df: DataFrame) -> tuple[DataFrame, int]:
        """
        Validate transformed data and return valid records
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (valid DataFrame, error count)
        """
        self.logger.info("Validating transformed data")
        
        # Define validation conditions
        valid_df = analytics_df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        total_count = analytics_df.count()
        valid_count = valid_df.count()
        error_count = total_count - valid_count
        
        if error_count > 0:
            self.logger.warning(f"Found {error_count} invalid records out of {total_count}")
        else:
            self.logger.info(f"All {total_count} records are valid")
        
        return valid_df, error_count