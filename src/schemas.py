"""
PySpark Schema Definitions for Sales ETL System

This module contains all StructType schema definitions converted from ABAP type definitions.
Schemas include raw sales data, analytics data, ETL logs, and configuration structures.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType,
)


class ETLSchemas:
    """
    Central schema definitions for the ETL system.
    All schemas are immutable and thread-safe.
    """

    # Raw Sales Data Schema (from ty_raw_sales)
    RAW_SALES_SCHEMA = StructType([
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
        StructField("created_by", StringType(), nullable=True),
    ])

    # Analytics Data Schema (from ty_analytics)
    ANALYTICS_SCHEMA = StructType([
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
        StructField("loaded_at", TimestampType(), nullable=True),
        StructField("loaded_by", StringType(), nullable=True),
    ])

    # ETL Log Schema (from ty_etl_log)
    ETL_LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), nullable=False),
        StructField("etl_run_id", StringType(), nullable=False),
        StructField("execution_date", DateType(), nullable=False),
        StructField("execution_time", StringType(), nullable=False),
        StructField("process_step", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("records_processed", IntegerType(), nullable=True),
        StructField("records_success", IntegerType(), nullable=True),
        StructField("records_error", IntegerType(), nullable=True),
        StructField("message", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("created_by", StringType(), nullable=True),
    ])

    # ETL Configuration Schema (from ty_etl_config)
    ETL_CONFIG_SCHEMA = StructType([
        StructField("batch_size", IntegerType(), nullable=False),
        StructField("commit_interval", IntegerType(), nullable=False),
        StructField("parallel_jobs", IntegerType(), nullable=False),
        StructField("retry_attempts", IntegerType(), nullable=False),
        StructField("timeout_seconds", IntegerType(), nullable=False),
    ])

    # ETL Statistics Schema (from ty_etl_statistics)
    ETL_STATISTICS_SCHEMA = StructType([
        StructField("total_records", IntegerType(), nullable=False),
        StructField("success_records", IntegerType(), nullable=False),
        StructField("error_records", IntegerType(), nullable=False),
        StructField("warning_records", IntegerType(), nullable=False),
        StructField("start_time", TimestampType(), nullable=False),
        StructField("end_time", TimestampType(), nullable=True),
        StructField("duration_seconds", IntegerType(), nullable=True),
    ])

    # Execution Result Schema (from zif_etl_component.ty_execution_result)
    EXECUTION_RESULT_SCHEMA = StructType([
        StructField("success", BooleanType(), nullable=False),
        StructField("records_total", IntegerType(), nullable=False),
        StructField("records_success", IntegerType(), nullable=False),
        StructField("records_error", IntegerType(), nullable=False),
        StructField("message", StringType(), nullable=True),
    ])

    @classmethod
    def get_schema(cls, schema_name: str) -> StructType:
        """
        Get a schema by name.

        Args:
            schema_name: Name of the schema to retrieve

        Returns:
            StructType schema definition

        Raises:
            ValueError: If schema name is not found
        """
        schema_map = {
            "raw_sales": cls.RAW_SALES_SCHEMA,
            "analytics": cls.ANALYTICS_SCHEMA,
            "etl_log": cls.ETL_LOG_SCHEMA,
            "etl_config": cls.ETL_CONFIG_SCHEMA,
            "etl_statistics": cls.ETL_STATISTICS_SCHEMA,
            "execution_result": cls.EXECUTION_RESULT_SCHEMA,
        }

        if schema_name not in schema_map:
            raise ValueError(
                f"Schema '{schema_name}' not found. "
                f"Available schemas: {', '.join(schema_map.keys())}"
            )

        return schema_map[schema_name]

    @classmethod
    def validate_dataframe(cls, df, schema_name: str) -> bool:
        """
        Validate that a DataFrame matches an expected schema.

        Args:
            df: PySpark DataFrame to validate
            schema_name: Name of the expected schema

        Returns:
            True if schema matches, False otherwise
        """
        expected_schema = cls.get_schema(schema_name)
        return df.schema == expected_schema

    @classmethod
    def get_field_names(cls, schema_name: str) -> list:
        """
        Get list of field names for a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List of field names
        """
        schema = cls.get_schema(schema_name)
        return [field.name for field in schema.fields]

    @classmethod
    def get_required_fields(cls, schema_name: str) -> list:
        """
        Get list of required (non-nullable) fields for a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List of required field names
        """
        schema = cls.get_schema(schema_name)
        return [field.name for field in schema.fields if not field.nullable]


# Constants (from ZCL_ETL_CONSTANTS)
class ETLConstants:
    """
    Constants and configuration values for ETL system.
    Converted from ABAP ZCL_ETL_CONSTANTS class.
    """

    # Status codes
    class Status:
        NEW = "N"
        PROCESSED = "P"
        ERROR = "E"
        WARNING = "W"
        SUCCESS = "S"
        INFO = "I"

    # ETL process steps
    class Step:
        INIT = "INIT"
        EXTRACT = "EXTRACT"
        TRANSFORM = "TRANSFORM"
        LOAD = "LOAD"
        VALIDATE = "VALIDATE"
        COMPLETE = "COMPLETE"
        ERROR = "ERROR"

    # Sale categories
    class Category:
        HIGH = "HIGH"
        MEDIUM = "MEDIUM"
        LOW = "LOW"

    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = 0.05
    DISCOUNT_RATE_TIER2 = 0.10

    # Business rules - Tax rate
    TAX_RATE = 0.08

    # Business rules - Cost ratio
    COST_RATIO = 0.60

    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = 2000.00
    CATEGORY_MEDIUM_THRESHOLD = 500.00

    # ETL configuration defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600

    # ID prefixes
    PREFIX_ETL_RUN = "ETL"
    PREFIX_LOG_ID = "LOG"
    PREFIX_ANALYTICS_ID = "ANL"

    # Message texts
    MSG_INIT_SUCCESS = "ETL process initialized successfully"
    MSG_EXTRACT_START = "Starting data extraction"
    MSG_EXTRACT_COMPLETE = "Data extraction completed"
    MSG_TRANSFORM_START = "Starting data transformation"
    MSG_TRANSFORM_COMPLETE = "Data transformation completed"
    MSG_LOAD_START = "Starting data load"
    MSG_LOAD_COMPLETE = "Data load completed"
    MSG_ETL_COMPLETE = "ETL process completed successfully"
    MSG_ETL_ERROR = "ETL process failed"