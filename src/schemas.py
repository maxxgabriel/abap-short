"""
PySpark schema definitions migrated from ABAP ZETL_TYPES.
Defines StructType schemas for raw sales, analytics, and ETL log data.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)


class ETLSchemas:
    """Central repository for all ETL PySpark schemas."""

    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Raw sales data structure from ZSALES_RAW table.
        
        Migrated from ZETL_TYPES.ty_raw_sales:
        - trans_id: char10 -> StringType
        - trans_date: dats -> DateType
        - customer_id: char10 -> StringType
        - product_id: char10 -> StringType
        - quantity: int -> IntegerType
        - unit_price: p(16,2) -> DecimalType(16,2)
        - currency: waers -> StringType
        - sales_rep: char20 -> StringType
        - region: char10 -> StringType
        - status: char1 -> StringType
        - created_at: timestampl -> TimestampType
        - created_by: syuname -> StringType
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
            StructField("created_by", StringType(), nullable=True),
        ])

    @staticmethod
    def analytics_schema() -> StructType:
        """
        Analytics data structure for ZSALES_ANALYTICS table.
        
        Migrated from ZETL_TYPES.ty_analytics:
        - analytics_id: char20 -> StringType
        - trans_date: dats -> DateType
        - customer_id: char10 -> StringType
        - product_id: char10 -> StringType
        - total_quantity: int -> IntegerType
        - gross_amount: p(16,2) -> DecimalType(16,2)
        - net_amount: p(16,2) -> DecimalType(16,2)
        - discount_amount: p(16,2) -> DecimalType(16,2)
        - tax_amount: p(16,2) -> DecimalType(16,2)
        - currency: waers -> StringType
        - sales_rep: char20 -> StringType
        - region: char10 -> StringType
        - profit_margin: p(5,2) -> DecimalType(5,2)
        - category: char10 -> StringType
        - etl_run_id: char20 -> StringType
        - loaded_at: timestampl -> TimestampType
        - loaded_by: syuname -> StringType
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
            StructField("loaded_by", StringType(), nullable=True),
        ])

    @staticmethod
    def etl_log_schema() -> StructType:
        """
        ETL log structure for ZETL_LOG table.
        
        Migrated from ZETL_TYPES.ty_etl_log:
        - log_id: char20 -> StringType
        - etl_run_id: char20 -> StringType
        - execution_date: dats -> DateType
        - execution_time: tims -> StringType (HH:MM:SS)
        - process_step: char20 -> StringType
        - status: char1 -> StringType
        - records_processed: int -> IntegerType
        - records_success: int -> IntegerType
        - records_error: int -> IntegerType
        - message: char255 -> StringType
        - created_at: timestampl -> TimestampType
        - created_by: syuname -> StringType
        """
        return StructType([
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