"""
Data loader module with field validation.

This module implements the loader component with validation rules:
- Required fields: analytics_id, customer_id, product_id
- gross_amount > 0
- non-null currency
- category in ('HIGH', 'MEDIUM', 'LOW')
"""

from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
import logging
from datetime import datetime


class LoadError(Exception):
    """Custom exception for data loading errors."""
    
    def __init__(self, message: str, record_id: str = None, error_type: str = None):
        self.message = message
        self.record_id = record_id
        self.error_type = error_type
        super().__init__(self.message)
    
    def __str__(self):
        if self.record_id:
            return f"LoadError [{self.error_type}] for record {self.record_id}: {self.message}"
        return f"LoadError [{self.error_type}]: {self.message}"


class DataLoader:
    """
    Data loader component with comprehensive field validation.
    
    Validates analytics data against business rules before loading to target.
    Raises LoadError exceptions on validation failures.
    """
    
    # Valid category values
    VALID_CATEGORIES = {'HIGH', 'MEDIUM', 'LOW'}
    
    # Required fields
    REQUIRED_FIELDS = ['analytics_id', 'customer_id', 'product_id', 'gross_amount', 'currency', 'category']
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], logger: logging.Logger = None):
        """
        Initialize the DataLoader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.load_stats = {
            'records_processed': 0,
            'records_success': 0,
            'records_error': 0,
            'validation_errors': []
        }
    
    def get_target_schema(self) -> StructType:
        """
        Define the schema for the target analytics table.
        
        Returns:
            StructType: Target table schema
        """
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=True),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=True),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=True),
            StructField("discount_amount", DecimalType(16, 2), nullable=True),
            StructField("tax_amount", DecimalType(16, 2), nullable=True),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=True),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    def validate_required_fields(self, df: DataFrame) -> DataFrame:
        """
        Validate that required fields are not null.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validation status
            
        Raises:
            LoadError: If required fields are missing from schema
        """
        self.logger.info("Validating required fields...")
        
        # Check schema contains required fields
        df_columns = set(df.columns)
        missing_columns = set(self.REQUIRED_FIELDS) - df_columns
        if missing_columns:
            raise LoadError(
                f"Missing required columns: {missing_columns}",
                error_type="SCHEMA_ERROR"
            )
        
        # Add validation flags for each required field
        validation_expr = F.lit(True)
        for field in self.REQUIRED_FIELDS:
            validation_expr = validation_expr & F.col(field).isNotNull()
        
        df_validated = df.withColumn("_required_fields_valid", validation_expr)
        
        # Count invalid records
        invalid_count = df_validated.filter(~F.col("_required_fields_valid")).count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with null required fields")
            self._collect_validation_errors(
                df_validated.filter(~F.col("_required_fields_valid")),
                "REQUIRED_FIELD_NULL"
            )
        
        return df_validated
    
    def validate_gross_amount(self, df: DataFrame) -> DataFrame:
        """
        Validate that gross_amount is greater than 0.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validation status
        """
        self.logger.info("Validating gross_amount > 0...")
        
        df_validated = df.withColumn(
            "_gross_amount_valid",
            (F.col("gross_amount").isNotNull()) & (F.col("gross_amount") > 0)
        )
        
        invalid_count = df_validated.filter(~F.col("_gross_amount_valid")).count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with invalid gross_amount")
            self._collect_validation_errors(
                df_validated.filter(~F.col("_gross_amount_valid")),
                "GROSS_AMOUNT_INVALID"
            )
        
        return df_validated
    
    def validate_currency(self, df: DataFrame) -> DataFrame:
        """
        Validate that currency is not null and not empty.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validation status
        """
        self.logger.info("Validating currency field...")
        
        df_validated = df.withColumn(
            "_currency_valid",
            (F.col("currency").isNotNull()) & 
            (F.trim(F.col("currency")) != "") &
            (F.length(F.col("currency")) > 0)
        )
        
        invalid_count = df_validated.filter(~F.col("_currency_valid")).count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with invalid currency")
            self._collect_validation_errors(
                df_validated.filter(~F.col("_currency_valid")),
                "CURRENCY_INVALID"
            )
        
        return df_validated
    
    def validate_category(self, df: DataFrame) -> DataFrame:
        """
        Validate that category is in allowed values (HIGH, MEDIUM, LOW).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validation status
        """
        self.logger.info("Validating category values...")
        
        df_validated = df.withColumn(
            "_category_valid",
            (F.col("category").isNotNull()) & 
            (F.col("category").isin(list(self.VALID_CATEGORIES)))
        )
        
        invalid_count = df_validated.filter(~F.col("_category_valid")).count()
        if invalid_count > 0:
            self.logger.warning(f"Found {invalid_count} records with invalid category")
            self._collect_validation_errors(
                df_validated.filter(~F.col("_category_valid")),
                "CATEGORY_INVALID"
            )
        
        return df_validated
    
    def validate_all(self, df: DataFrame) -> DataFrame:
        """
        Run all validation rules on the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with all validation flags and overall validity
            
        Raises:
            LoadError: If validation configuration is invalid
        """
        self.logger.info("Starting comprehensive data validation...")
        
        # Apply all validations
        df_validated = df
        df_validated = self.validate_required_fields(df_validated)
        df_validated = self.validate_gross_amount(df_validated)
        df_validated = self.validate_currency(df_validated)
        df_validated = self.validate_category(df_validated)
        
        # Combine all validation flags
        df_validated = df_validated.withColumn(
            "_is_valid",
            F.col("_required_fields_valid") &
            F.col("_gross_amount_valid") &
            F.col("_currency_valid") &
            F.col("_category_valid")
        )
        
        # Calculate validation statistics
        total_count = df_validated.count()
        valid_count = df_validated.filter(F.col("_is_valid")).count()
        invalid_count = total_count - valid_count
        
        self.logger.info(f"Validation complete: {valid_count}/{total_count} valid records")
        
        if invalid_count > 0:
            self.logger.warning(f"{invalid_count} records failed validation")
        
        self.load_stats['records_processed'] = total_count
        self.load_stats['records_success'] = valid_count
        self.load_stats['records_error'] = invalid_count
        
        return df_validated
    
    def _collect_validation_errors(self, invalid_df: DataFrame, error_type: str, limit: int = 100):
        """
        Collect validation error details for logging.
        
        Args:
            invalid_df: DataFrame with invalid records
            error_type: Type of validation error
            limit: Maximum number of errors to collect
        """
        error_records = invalid_df.select("analytics_id").limit(limit).collect()
        for row in error_records:
            self.load_stats['validation_errors'].append({
                'analytics_id': row['analytics_id'],
                'error_type': error_type,
                'timestamp': datetime.now().isoformat()
            })
    
    def load_data(self, df: DataFrame, target_table: str = None, mode: str = "append") -> Dict[str, Any]:
        """
        Load validated data to target table.
        
        Args:
            df: Input DataFrame
            target_table: Target table name (from config if not provided)
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Dictionary with load statistics
            
        Raises:
            LoadError: If validation fails or load operation fails
        """
        self.logger.info("Starting data load process...")
        
        # Get target table from config if not provided
        if target_table is None:
            target_table = self.config.get('target_table', 'sales_analytics')
        
        # Validate the data
        df_validated = self.validate_all(df)
        
        # Get valid and invalid records
        df_valid = df_validated.filter(F.col("_is_valid"))
        df_invalid = df_validated.filter(~F.col("_is_valid"))
        
        # Handle invalid records based on configuration
        strict_mode = self.config.get('validation', {}).get('strict_mode', False)
        
        if strict_mode and self.load_stats['records_error'] > 0:
            # In strict mode, fail if any validation errors
            error_details = self._format_validation_errors()
            raise LoadError(
                f"Validation failed in strict mode. {self.load_stats['records_error']} invalid records found.\n{error_details}",
                error_type="STRICT_VALIDATION_FAILED"
            )
        
        # Remove validation columns before loading
        validation_cols = [col for col in df_valid.columns if col.startswith('_')]
        df_to_load = df_valid.drop(*validation_cols)
        
        # Add metadata columns
        df_to_load = df_to_load.withColumn("loaded_at", F.current_timestamp())
        df_to_load = df_to_load.withColumn("loaded_by", F.lit(self.config.get('user', 'system')))
        
        try:
            # Write to target
            target_path = self.config.get('paths', {}).get('target', f"output/{target_table}")
            
            self.logger.info(f"Writing {self.load_stats['records_success']} valid records to {target_path}")
            
            df_to_load.write \
                .format(self.config.get('target_format', 'parquet')) \
                .mode(mode) \
                .save(target_path)
            
            self.logger.info(f"Successfully loaded {self.load_stats['records_success']} records")
            
            # Handle invalid records if any
            if self.load_stats['records_error'] > 0:
                self._handle_invalid_records(df_invalid, validation_cols)
            
            # Return statistics
            return self._build_load_result()
            
        except Exception as e:
            self.logger.error(f"Load operation failed: {str(e)}")
            raise LoadError(
                f"Failed to load data to {target_path}: {str(e)}",
                error_type="LOAD_OPERATION_FAILED"
            )
    
    def _handle_invalid_records(self, df_invalid: DataFrame, validation_cols: List[str]):
        """
        Handle invalid records based on configuration.
        
        Args:
            df_invalid: DataFrame with invalid records
            validation_cols: List of validation column names
        """
        error_handling = self.config.get('validation', {}).get('error_handling', 'log')
        
        if error_handling == 'quarantine':
            # Write invalid records to quarantine location
            quarantine_path = self.config.get('paths', {}).get('quarantine', 'output/quarantine')
            
            self.logger.info(f"Writing {self.load_stats['records_error']} invalid records to quarantine")
            
            df_invalid.write \
                .format('parquet') \
                .mode('append') \
                .save(quarantine_path)
        
        elif error_handling == 'log':
            # Just log the invalid record IDs
            self.logger.warning(f"Invalid record IDs: {self.load_stats['validation_errors'][:10]}")
    
    def _format_validation_errors(self) -> str:
        """
        Format validation errors for error message.
        
        Returns:
            Formatted error string
        """
        error_summary = {}
        for error in self.load_stats['validation_errors']:
            error_type = error['error_type']
            error_summary[error_type] = error_summary.get(error_type, 0) + 1
        
        lines = ["Validation Error Summary:"]
        for error_type, count in error_summary.items():
            lines.append(f"  - {error_type}: {count} records")
        
        return "\n".join(lines)
    
    def _build_load_result(self) -> Dict[str, Any]:
        """
        Build the final load result dictionary.
        
        Returns:
            Dictionary with load results and statistics
        """
        return {
            'success': True,
            'records_processed': self.load_stats['records_processed'],
            'records_loaded': self.load_stats['records_success'],
            'records_failed': self.load_stats['records_error'],
            'validation_errors': self.load_stats['validation_errors'],
            'timestamp': datetime.now().isoformat()
        }
    
    def get_load_statistics(self) -> Dict[str, Any]:
        """
        Get current load statistics.
        
        Returns:
            Dictionary with load statistics
        """
        return self.load_stats.copy()


def create_loader(spark: SparkSession, config: Dict[str, Any], logger: logging.Logger = None) -> DataLoader:
    """
    Factory function to create a DataLoader instance.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        logger: Logger instance
        
    Returns:
        DataLoader instance
    """
    return DataLoader(spark, config, logger)