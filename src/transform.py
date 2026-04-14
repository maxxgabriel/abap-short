"""
PySpark transformation module with comprehensive data validation.

This module implements data transformation logic with validation for:
- Required fields
- Currency codes
- Category enumerations
- Data quality checks
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class DataTransformer:
    """Handles data transformation with comprehensive validation."""
    
    def __init__(self, config: Dict):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Configuration dictionary containing validation rules
        """
        self.config = config
        self.validation_config = config.get('validation', {})
        self.business_rules = config.get('business_rules', {})
        
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics output data.
        
        Returns:
            StructType schema for analytics data
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
            StructField("sales_rep", StringType(), False),
            StructField("region", StringType(), False),
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("validation_status", StringType(), False),
            StructField("validation_errors", StringType(), True)
        ])
    
    def validate_required_fields(self, df: DataFrame) -> DataFrame:
        """
        Validate that required fields are not null or empty.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validation_errors column added
        """
        required_fields = self.validation_config.get('required_fields', [])
        
        logger.info(f"Validating {len(required_fields)} required fields")
        
        # Build validation expression for required fields
        validation_conditions = []
        error_messages = []
        
        for field in required_fields:
            if field in df.columns:
                condition = F.col(field).isNull() | (F.trim(F.col(field)) == "")
                validation_conditions.append(condition)
                error_messages.append(F.when(condition, F.lit(f"Missing required field: {field}")))
        
        # Combine all error messages
        if error_messages:
            df = df.withColumn(
                "required_field_errors",
                F.concat_ws("; ", *[msg for msg in error_messages if msg is not None])
            )
        else:
            df = df.withColumn("required_field_errors", F.lit(None).cast(StringType()))
        
        return df
    
    def validate_currency_codes(self, df: DataFrame) -> DataFrame:
        """
        Validate currency codes against allowed list.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with currency validation errors
        """
        valid_currencies = self.validation_config.get('valid_currencies', ['USD', 'EUR', 'GBP'])
        
        logger.info(f"Validating currency codes against: {valid_currencies}")
        
        df = df.withColumn(
            "currency_errors",
            F.when(
                ~F.col("currency").isin(valid_currencies),
                F.concat(F.lit("Invalid currency code: "), F.col("currency"))
            ).otherwise(F.lit(None))
        )
        
        return df
    
    def validate_category_enumerations(self, df: DataFrame) -> DataFrame:
        """
        Validate category values against allowed enumerations.
        
        Args:
            df: Input DataFrame with category column
            
        Returns:
            DataFrame with category validation errors
        """
        valid_categories = self.validation_config.get('valid_categories', ['HIGH', 'MEDIUM', 'LOW'])
        
        logger.info(f"Validating categories against: {valid_categories}")
        
        df = df.withColumn(
            "category_errors",
            F.when(
                ~F.col("category").isin(valid_categories),
                F.concat(F.lit("Invalid category: "), F.col("category"))
            ).otherwise(F.lit(None))
        )
        
        return df
    
    def validate_numeric_ranges(self, df: DataFrame) -> DataFrame:
        """
        Validate numeric fields are within acceptable ranges.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with numeric validation errors
        """
        numeric_validations = self.validation_config.get('numeric_ranges', {})
        
        error_conditions = []
        
        for field, rules in numeric_validations.items():
            if field in df.columns:
                min_val = rules.get('min')
                max_val = rules.get('max')
                
                if min_val is not None:
                    error_conditions.append(
                        F.when(
                            F.col(field) < min_val,
                            F.lit(f"{field} below minimum: {min_val}")
                        )
                    )
                
                if max_val is not None:
                    error_conditions.append(
                        F.when(
                            F.col(field) > max_val,
                            F.lit(f"{field} above maximum: {max_val}")
                        )
                    )
        
        if error_conditions:
            df = df.withColumn(
                "numeric_errors",
                F.concat_ws("; ", *[cond for cond in error_conditions if cond is not None])
            )
        else:
            df = df.withColumn("numeric_errors", F.lit(None).cast(StringType()))
        
        return df
    
    def validate_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Validate business-specific rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rule validation errors
        """
        error_conditions = []
        
        # Validate discount doesn't exceed gross amount
        error_conditions.append(
            F.when(
                F.col("discount_amount") > F.col("gross_amount"),
                F.lit("Discount exceeds gross amount")
            )
        )
        
        # Validate net amount is positive
        error_conditions.append(
            F.when(
                F.col("net_amount") <= 0,
                F.lit("Net amount must be positive")
            )
        )
        
        # Validate profit margin is reasonable
        max_margin = self.business_rules.get('max_profit_margin', 100)
        error_conditions.append(
            F.when(
                F.col("profit_margin") > max_margin,
                F.lit(f"Profit margin exceeds {max_margin}%")
            )
        )
        
        df = df.withColumn(
            "business_rule_errors",
            F.concat_ws("; ", *[cond for cond in error_conditions if cond is not None])
        )
        
        return df
    
    def combine_validation_errors(self, df: DataFrame) -> DataFrame:
        """
        Combine all validation errors into single column.
        
        Args:
            df: DataFrame with individual error columns
            
        Returns:
            DataFrame with combined validation_errors column
        """
        error_columns = [
            "required_field_errors",
            "currency_errors",
            "category_errors",
            "numeric_errors",
            "business_rule_errors"
        ]
        
        # Filter out null/empty error messages
        non_null_errors = [
            F.when(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""), F.col(col))
            for col in error_columns
            if col in df.columns
        ]
        
        df = df.withColumn(
            "validation_errors",
            F.concat_ws(" | ", *non_null_errors)
        )
        
        # Set validation status
        df = df.withColumn(
            "validation_status",
            F.when(
                (F.col("validation_errors").isNull()) | (F.trim(F.col("validation_errors")) == ""),
                F.lit("VALID")
            ).otherwise(F.lit("INVALID"))
        )
        
        # Drop intermediate error columns
        df = df.drop(*error_columns)
        
        return df
    
    def calculate_analytics(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Calculate analytics fields from raw data.
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Transformed DataFrame with analytics
        """
        logger.info("Calculating analytics fields")
        
        # Get business rules
        discount_tier1_qty = self.business_rules.get('discount_qty_tier1', 10)
        discount_tier2_qty = self.business_rules.get('discount_qty_tier2', 15)
        discount_rate_tier1 = self.business_rules.get('discount_rate_tier1', 0.05)
        discount_rate_tier2 = self.business_rules.get('discount_rate_tier2', 0.10)
        tax_rate = self.business_rules.get('tax_rate', 0.08)
        cost_ratio = self.business_rules.get('cost_ratio', 0.60)
        category_high_threshold = self.business_rules.get('category_high_threshold', 2000.00)
        category_medium_threshold = self.business_rules.get('category_medium_threshold', 500.00)
        
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > discount_tier2_qty,
                F.col("gross_amount") * F.lit(discount_rate_tier2)
            ).when(
                F.col("quantity") > discount_tier1_qty,
                F.col("gross_amount") * F.lit(discount_rate_tier1)
            ).otherwise(F.lit(0.0))
        )
        
        # Calculate tax
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * F.lit(tax_rate)
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate profit margin
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * F.lit(cost_ratio)
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(F.lit(0.0))
        )
        
        # Categorize sales
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= category_high_threshold,
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= category_medium_threshold,
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
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
        df = df.withColumn("etl_run_id", F.lit(etl_run_id))
        
        # Rename quantity column
        df = df.withColumnRenamed("quantity", "total_quantity")
        
        # Drop temporary columns
        df = df.drop("cost_amount", "unit_price", "status", "trans_id")
        
        return df
    
    def transform(self, raw_df: DataFrame, etl_run_id: str) -> Tuple[DataFrame, Dict]:
        """
        Main transformation method with validation.
        
        Args:
            raw_df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Tuple of (transformed DataFrame, validation statistics)
        """
        logger.info("Starting data transformation with validation")
        
        # Calculate analytics
        df = self.calculate_analytics(raw_df, etl_run_id)
        
        # Apply validations
        df = self.validate_required_fields(df)
        df = self.validate_currency_codes(df)
        df = self.validate_category_enumerations(df)
        df = self.validate_numeric_ranges(df)
        df = self.validate_business_rules(df)
        
        # Combine validation errors
        df = self.combine_validation_errors(df)
        
        # Calculate validation statistics
        total_records = df.count()
        valid_records = df.filter(F.col("validation_status") == "VALID").count()
        invalid_records = total_records - valid_records
        
        validation_stats = {
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "validation_rate": (valid_records / total_records * 100) if total_records > 0 else 0
        }
        
        logger.info(f"Transformation complete. Valid: {valid_records}/{total_records} "
                   f"({validation_stats['validation_rate']:.2f}%)")
        
        return df, validation_stats


def transform_sales_data(spark: SparkSession, raw_df: DataFrame, 
                        config: Dict, etl_run_id: str) -> Tuple[DataFrame, Dict]:
    """
    Transform raw sales data with validation.
    
    Args:
        spark: SparkSession
        raw_df: Raw sales DataFrame
        config: Configuration dictionary
        etl_run_id: ETL run identifier
        
    Returns:
        Tuple of (transformed DataFrame, validation statistics)
    """
    transformer = DataTransformer(config)
    return transformer.transform(raw_df, etl_run_id)