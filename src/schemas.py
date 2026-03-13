"""
Schema definitions for ETL data structures
Migrated from ABAP ZETL_TYPES
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType
)


class ETLSchemas:
    """Schema definitions for ETL data structures"""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Schema for raw sales data
        Corresponds to ZETL_TYPES=>ty_raw_sales
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
        Schema for analytics data
        Corresponds to ZETL_TYPES=>ty_analytics
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
        Schema for ETL log data
        Corresponds to ZETL_TYPES=>ty_etl_log
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
            StructField("created_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def get_schema(schema_name: str) -> StructType:
        """
        Get schema by name
        
        Args:
            schema_name: Name of the schema (raw_sales, analytics, etl_log)
        
        Returns:
            StructType schema
        """
        schemas = {
            'raw_sales': ETLSchemas.raw_sales_schema(),
            'analytics': ETLSchemas.analytics_schema(),
            'etl_log': ETLSchemas.etl_log_schema()
        }
        
        if schema_name not in schemas:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        return schemas[schema_name]