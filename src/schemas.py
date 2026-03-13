"""
PySpark schema definitions migrated from ABAP ZETL_TYPES structures.

This module contains all StructType schema definitions that map from ABAP
data structures to PySpark DataFrames with proper type conversions.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType
)


class ETLSchemas:
    """Container class for all ETL-related PySpark schemas."""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Schema for raw sales data (ABAP: ty_raw_sales).
        
        ABAP Type Mappings:
        - CHAR(n) -> StringType()
        - DATS -> DateType() with custom date parsing
        - I (integer) -> IntegerType()
        - P LENGTH n DECIMALS d -> DecimalType(n, d)
        - WAERS -> StringType() (currency code)
        - TIMESTAMPL -> TimestampType()
        - SYUNAME -> StringType()
        """
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def analytics_schema() -> StructType:
        """
        Schema for analytics data (ABAP: ty_analytics).
        
        Includes calculated fields from transformation logic:
        - gross_amount, net_amount, discount_amount, tax_amount
        - profit_margin, category
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
            StructField("profit_margin", DecimalType(5, 2), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        """
        Schema for ETL log entries (ABAP: ty_etl_log).
        
        Tracks execution metrics and status for each ETL step.
        """
        return StructType([
            StructField("log_id", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("execution_date", DateType(), nullable=False),
            StructField("execution_time", StringType(), nullable=False),  # TIMS as string HH:MM:SS
            StructField("process_step", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("records_processed", IntegerType(), nullable=False),
            StructField("records_success", IntegerType(), nullable=False),
            StructField("records_error", IntegerType(), nullable=False),
            StructField("message", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def etl_config_schema() -> StructType:
        """
        Schema for ETL configuration (ABAP: ty_etl_config).
        
        Runtime configuration parameters for batch processing.
        """
        return StructType([
            StructField("batch_size", IntegerType(), nullable=False),
            StructField("commit_interval", IntegerType(), nullable=False),
            StructField("parallel_jobs", IntegerType(), nullable=False),
            StructField("retry_attempts", IntegerType(), nullable=False),
            StructField("timeout_seconds", IntegerType(), nullable=False)
        ])
    
    @staticmethod
    def etl_statistics_schema() -> StructType:
        """
        Schema for ETL run statistics (ABAP: ty_etl_statistics).
        
        Summary metrics for completed ETL runs.
        """
        return StructType([
            StructField("total_records", IntegerType(), nullable=False),
            StructField("success_records", IntegerType(), nullable=False),
            StructField("error_records", IntegerType(), nullable=False),
            StructField("warning_records", IntegerType(), nullable=False),
            StructField("start_time", TimestampType(), nullable=False),
            StructField("end_time", TimestampType(), nullable=True),
            StructField("duration_seconds", IntegerType(), nullable=True)
        ])


class DateFormatHelper:
    """
    Helper utilities for ABAP DATS format conversion.
    
    ABAP DATS format: YYYYMMDD (string)
    PySpark DateType: date object
    """
    
    ABAP_DATE_FORMAT = "yyyyMMdd"
    ABAP_TIME_FORMAT = "HHmmss"
    
    @staticmethod
    def dats_to_date_expr(col_name: str) -> str:
        """
        Generate PySpark SQL expression to convert DATS string to DateType.
        
        Args:
            col_name: Name of the column containing DATS value
            
        Returns:
            SQL expression string for to_date conversion
            
        Example:
            >>> DateFormatHelper.dats_to_date_expr("trans_date")
            "to_date(trans_date, 'yyyyMMdd')"
        """
        return f"to_date({col_name}, '{DateFormatHelper.ABAP_DATE_FORMAT}')"
    
    @staticmethod
    def tims_to_time_expr(col_name: str) -> str:
        """
        Generate PySpark SQL expression to format TIMS to time string.
        
        ABAP TIMS: HHMMSS (6 digits)
        Output: HH:MM:SS string format
        
        Args:
            col_name: Name of the column containing TIMS value
            
        Returns:
            SQL expression for formatted time string
        """
        return f"""concat(
            substring({col_name}, 1, 2), ':',
            substring({col_name}, 3, 2), ':',
            substring({col_name}, 5, 2)
        )"""


class TypeMappings:
    """
    Documentation of ABAP to PySpark type mappings used in this migration.
    """
    
    MAPPINGS = {
        "CHAR(n)": "StringType()",
        "NUMC(n)": "StringType()",  # Numeric character, keep as string for leading zeros
        "DATS": "DateType()",  # Date: YYYYMMDD
        "TIMS": "StringType()",  # Time: HHMMSS, formatted as HH:MM:SS
        "I": "IntegerType()",  # 4-byte integer
        "INT8": "LongType()",  # 8-byte integer
        "P LENGTH n DECIMALS d": "DecimalType(n, d)",  # Packed decimal
        "F": "DoubleType()",  # Floating point
        "WAERS": "StringType()",  # Currency key (5 chars)
        "TIMESTAMPL": "TimestampType()",  # UTC timestamp
        "SYUNAME": "StringType()",  # Username (12 chars)
        "ABAP_BOOL": "BooleanType()",  # Boolean flag (X or blank)
    }
    
    @staticmethod
    def get_mapping_docs() -> str:
        """Return formatted documentation of type mappings."""
        lines = ["ABAP to PySpark Type Mappings:", "=" * 50]
        for abap_type, pyspark_type in TypeMappings.MAPPINGS.items():
            lines.append(f"{abap_type:30} -> {pyspark_type}")
        return "\n".join(lines)


# Module-level convenience functions
def get_raw_sales_schema() -> StructType:
    """Get raw sales schema."""
    return ETLSchemas.raw_sales_schema()


def get_analytics_schema() -> StructType:
    """Get analytics schema."""
    return ETLSchemas.analytics_schema()


def get_etl_log_schema() -> StructType:
    """Get ETL log schema."""
    return ETLSchemas.etl_log_schema()


def get_etl_config_schema() -> StructType:
    """Get ETL config schema."""
    return ETLSchemas.etl_config_schema()


def get_etl_statistics_schema() -> StructType:
    """Get ETL statistics schema."""
    return ETLSchemas.etl_statistics_schema()