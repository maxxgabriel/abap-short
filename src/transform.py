"""
ETL Transformer Module
Transforms raw sales data into analytics format with business logic calculations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Dict, Any
import yaml
import logging
from datetime import datetime


class ETLTransformer:
    """Transforms raw sales data applying business rules and calculations."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize transformer with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics output data.
        
        Returns:
            StructType schema for analytics DataFrame
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
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
        ])
    
    def transform_data(
        self, 
        raw_df: DataFrame, 
        etl_run_id: str
    ) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed analytics DataFrame
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Step 1: Calculate gross amount
            df = self._calculate_gross_amount(raw_df)
            
            # Step 2: Apply tiered discount rules
            df = self._apply_discount_rules(df)
            
            # Step 3: Calculate tax
            df = self._calculate_tax(df)
            
            # Step 4: Calculate net amount
            df = self._calculate_net_amount(df)
            
            # Step 5: Calculate profit margin
            df = self._calculate_profit_margin(df)
            
            # Step 6: Categorize sales
            df = self._categorize_sales(df)
            
            # Step 7: Generate analytics ID and add metadata
            df = self._add_metadata(df, etl_run_id)
            
            # Step 8: Select and order final columns
            df = self._select_final_columns(df)
            
            record_count = df.count()
            self.logger.info(f"Transformation completed: {record_count} records processed")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _calculate_gross_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate gross amount (quantity * unit_price).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with gross_amount column
        """
        self.logger.info("Calculating gross amount")
        
        return df.withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(16, 2))
        )
    
    def _apply_discount_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply tiered discount rules based on quantity.
        
        Business Rules:
        - Quantity > 15: 10% discount
        - Quantity > 10: 5% discount
        - Otherwise: No discount
        
        Args:
            df: Input DataFrame with gross_amount
            
        Returns:
            DataFrame with discount_amount column
        """
        self.logger.info("Applying tiered discount rules")
        
        discount_config = self.config['business_rules']['discount']
        tier1_qty = discount_config['tier1_quantity']
        tier2_qty = discount_config['tier2_quantity']
        tier1_rate = discount_config['tier1_rate']
        tier2_rate = discount_config['tier2_rate']
        
        return df.withColumn(
            "discount_amount",
            F.when(F.col("quantity") > tier2_qty, 
                   F.col("gross_amount") * F.lit(tier2_rate))
            .when(F.col("quantity") > tier1_qty,
                  F.col("gross_amount") * F.lit(tier1_rate))
            .otherwise(F.lit(0.0))
            .cast(DecimalType(16, 2))
        )
    
    def _calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax on (gross_amount - discount_amount).
        
        Tax rate from config (default 8%).
        
        Args:
            df: Input DataFrame with gross_amount and discount_amount
            
        Returns:
            DataFrame with tax_amount column
        """
        self.logger.info("Calculating tax amount")
        
        tax_rate = self.config['business_rules']['tax_rate']
        
        return df.withColumn(
            "tax_amount",
            ((F.col("gross_amount") - F.col("discount_amount")) * F.lit(tax_rate))
            .cast(DecimalType(16, 2))
        )
    
    def _calculate_net_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate net amount (gross - discount + tax).
        
        Args:
            df: Input DataFrame with gross_amount, discount_amount, tax_amount
            
        Returns:
            DataFrame with net_amount column
        """
        self.logger.info("Calculating net amount")
        
        return df.withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount"))
            .cast(DecimalType(16, 2))
        )
    
    def _calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin percentage.
        
        Formula: ((net_amount - cost) / net_amount) * 100
        Assumes cost is 60% of gross amount (configurable).
        
        Args:
            df: Input DataFrame with net_amount and gross_amount
            
        Returns:
            DataFrame with profit_margin column
        """
        self.logger.info("Calculating profit margin")
        
        cost_ratio = self.config['business_rules']['cost_ratio']
        
        return df.withColumn(
            "cost_amount",
            (F.col("gross_amount") * F.lit(cost_ratio)).cast(DecimalType(16, 2))
        ).withColumn(
            "profit_margin",
            F.when(F.col("net_amount") > 0,
                   (((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
                   .cast(DecimalType(5, 2)))
            .otherwise(F.lit(0.0))
        ).drop("cost_amount")
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount thresholds.
        
        Categories:
        - HIGH: gross_amount >= 2000
        - MEDIUM: gross_amount >= 500
        - LOW: otherwise
        
        Args:
            df: Input DataFrame with gross_amount
            
        Returns:
            DataFrame with category column
        """
        self.logger.info("Categorizing sales")
        
        category_config = self.config['business_rules']['category_thresholds']
        high_threshold = category_config['high']
        medium_threshold = category_config['medium']
        
        return df.withColumn(
            "category",
            F.when(F.col("gross_amount") >= high_threshold, F.lit("HIGH"))
            .when(F.col("gross_amount") >= medium_threshold, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
        )
    
    def _add_metadata(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add metadata columns: analytics_id, etl_run_id, loaded_at.
        
        Args:
            df: Input DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame with metadata columns
        """
        self.logger.info("Adding metadata columns")
        
        return df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        ).withColumn(
            "etl_run_id",
            F.lit(etl_run_id)
        ).withColumn(
            "loaded_at",
            F.current_timestamp()
        )
    
    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        """
        Select and order final columns for analytics output.
        
        Args:
            df: Input DataFrame with all computed columns
            
        Returns:
            DataFrame with final column selection
        """
        return df.select(
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
            "etl_run_id",
            "loaded_at"
        )
    
    def validate_transformed_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate transformed data quality.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating transformed data")
        
        total_records = df.count()
        
        # Check for nulls in critical fields
        null_checks = {
            "analytics_id": df.filter(F.col("analytics_id").isNull()).count(),
            "customer_id": df.filter(F.col("customer_id").isNull()).count(),
            "product_id": df.filter(F.col("product_id").isNull()).count(),
            "gross_amount": df.filter(F.col("gross_amount").isNull()).count(),
            "category": df.filter(F.col("category").isNull()).count(),
        }
        
        # Check for invalid amounts
        invalid_amounts = df.filter(F.col("gross_amount") <= 0).count()
        
        # Check for invalid categories
        invalid_categories = df.filter(
            ~F.col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()
        
        validation_results = {
            "total_records": total_records,
            "null_checks": null_checks,
            "invalid_amounts": invalid_amounts,
            "invalid_categories": invalid_categories,
            "is_valid": (
                sum(null_checks.values()) == 0 and
                invalid_amounts == 0 and
                invalid_categories == 0
            )
        }
        
        self.logger.info(f"Validation results: {validation_results}")
        
        return validation_results


def main():
    """Main execution function for standalone testing."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL Transformer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Sample data schema
    raw_schema = StructType([
        StructField("trans_id", StringType(), False),
        StructField("trans_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), False),
    ])
    
    # Create sample data
    sample_data = [
        ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
        ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
        ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
        ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
    ]
    
    raw_df = spark.createDataFrame(sample_data, schema=raw_schema)
    
    # Initialize transformer
    transformer = ETLTransformer()
    
    # Transform data
    etl_run_id = f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"
    analytics_df = transformer.transform_data(raw_df, etl_run_id)
    
    # Show results
    analytics_df.show(truncate=False)
    
    # Validate
    validation = transformer.validate_transformed_data(analytics_df)
    print(f"\nValidation Results: {validation}")
    
    spark.stop()


if __name__ == "__main__":
    main()