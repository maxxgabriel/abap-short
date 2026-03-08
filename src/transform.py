"""
Data Transformation Module
Transforms raw sales data into analytics format with business rule calculations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, DecimalType
)
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, current_timestamp, concat
)
from typing import Dict, Any
import logging


class DataTransformer:
    """
    PySpark data transformation component implementing business logic.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], etl_run_id: str):
        """
        Initialize the DataTransformer.

        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary with business rules
            etl_run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(__name__)

        # Load business rules from config
        rules = self.config.get("business_rules", {})
        self.discount_qty_tier1 = rules.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = rules.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = rules.get("discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = rules.get("discount_rate_tier2", 0.10)
        self.tax_rate = rules.get("tax_rate", 0.08)
        self.cost_ratio = rules.get("cost_ratio", 0.60)
        self.category_high_threshold = rules.get("category_high_threshold", 2000.00)
        self.category_medium_threshold = rules.get("category_medium_threshold", 500.00)

    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data matching SAP ZSALES_ANALYTICS table.

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
        ])

    def calculate_discount(self, df: DataFrame) -> DataFrame:
        """
        Calculate discount amount based on quantity tiers.

        Args:
            df: DataFrame with quantity and unit_price columns

        Returns:
            DataFrame: DataFrame with discount_amount column added
        """
        df = df.withColumn(
            "gross_amount",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        df = df.withColumn(
            "discount_amount",
            when(col("quantity") > self.discount_qty_tier2,
                 spark_round(col("gross_amount") * lit(self.discount_rate_tier2), 2))
            .when(col("quantity") > self.discount_qty_tier1,
                  spark_round(col("gross_amount") * lit(self.discount_rate_tier1), 2))
            .otherwise(lit(0.00))
        )

        return df

    def calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax amount on gross minus discount.

        Args:
            df: DataFrame with gross_amount and discount_amount columns

        Returns:
            DataFrame: DataFrame with tax_amount column added
        """
        df = df.withColumn(
            "tax_amount",
            spark_round((col("gross_amount") - col("discount_amount")) * lit(self.tax_rate), 2)
        )

        return df

    def calculate_net_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate net amount as gross - discount + tax.

        Args:
            df: DataFrame with gross_amount, discount_amount, and tax_amount columns

        Returns:
            DataFrame: DataFrame with net_amount column added
        """
        df = df.withColumn(
            "net_amount",
            spark_round(
                col("gross_amount") - col("discount_amount") + col("tax_amount"),
                2
            )
        )

        return df

    def calculate_profit_margin(self, df: DataFrame) -> DataFrame:
        """
        Calculate profit margin as percentage.
        Assumes cost is cost_ratio of unit price.

        Args:
            df: DataFrame with quantity, unit_price, and net_amount columns

        Returns:
            DataFrame: DataFrame with profit_margin column added
        """
        df = df.withColumn(
            "cost_amount",
            spark_round(col("quantity") * col("unit_price") * lit(self.cost_ratio), 2)
        )

        df = df.withColumn(
            "profit_margin",
            when(col("net_amount") > 0,
                 spark_round(((col("net_amount") - col("cost_amount")) / col("net_amount")) * 100, 2))
            .otherwise(lit(0.00))
        )

        df = df.drop("cost_amount")

        return df

    def categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales into HIGH, MEDIUM, or LOW based on gross amount.

        Args:
            df: DataFrame with gross_amount column

        Returns:
            DataFrame: DataFrame with category column added
        """
        df = df.withColumn(
            "category",
            when(col("gross_amount") >= self.category_high_threshold, lit("HIGH"))
            .when(col("gross_amount") >= self.category_medium_threshold, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )

        return df

    def generate_analytics_id(self, df: DataFrame) -> DataFrame:
        """
        Generate unique analytics ID for each record.

        Args:
            df: DataFrame with trans_id column

        Returns:
            DataFrame: DataFrame with analytics_id column added
        """
        df = df.withColumn(
            "analytics_id",
            concat(lit("ANL"), col("trans_id"))
        )

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Main transformation method applying all business logic.

        Args:
            df: Raw sales DataFrame

        Returns:
            DataFrame: Transformed analytics DataFrame

        Raises:
            ValueError: If required columns are missing
        """
        self.logger.info("Starting data transformation")

        # Validate input columns
        required_columns = {
            "trans_id", "trans_date", "customer_id", "product_id",
            "quantity", "unit_price", "currency", "sales_rep", "region"
        }
        if not required_columns.issubset(set(df.columns)):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # Apply transformations in sequence
        df = self.calculate_discount(df)
        df = self.calculate_tax(df)
        df = self.calculate_net_amount(df)
        df = self.calculate_profit_margin(df)
        df = self.categorize_sales(df)
        df = self.generate_analytics_id(df)

        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.etl_run_id))

        # Select and rename columns to match analytics schema
        df_analytics = df.select(
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

        record_count = df_analytics.count()
        self.logger.info(f"Successfully transformed {record_count} records")

        return df_analytics

    def validate_transformation(self, df: DataFrame) -> bool:
        """
        Validate transformed data meets quality requirements.

        Args:
            df: Transformed DataFrame

        Returns:
            bool: True if validation passes
        """
        if df.isEmpty():
            self.logger.warning("Transformed DataFrame is empty")
            return False

        # Check for negative amounts
        negative_check = df.filter(
            (col("gross_amount") < 0) |
            (col("net_amount") < 0) |
            (col("tax_amount") < 0)
        ).count()

        if negative_check > 0:
            self.logger.error(f"Found {negative_check} records with negative amounts")
            return False

        # Check category values
        invalid_categories = df.filter(
            ~col("category").isin(["HIGH", "MEDIUM", "LOW"])
        ).count()

        if invalid_categories > 0:
            self.logger.error(f"Found {invalid_categories} records with invalid categories")
            return False

        self.logger.info("Transformation validation passed")
        return True