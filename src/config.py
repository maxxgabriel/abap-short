"""
Configuration module for Sales ETL System.
Migrated from ABAP ZETL_TOP and ZCL_ETL_CONSTANTS.
"""
from typing import Dict, Any
from decimal import Decimal
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType
)


class ETLConfig:
    """Central configuration class for ETL system."""
    
    # Status codes
    class Status:
        NEW = 'N'
        PROCESSED = 'P'
        ERROR = 'E'
        WARNING = 'W'
        SUCCESS = 'S'
        INFO = 'I'
    
    # ETL process steps
    class Step:
        INIT = 'INIT'
        EXTRACT = 'EXTRACT'
        TRANSFORM = 'TRANSFORM'
        LOAD = 'LOAD'
        VALIDATE = 'VALIDATE'
        COMPLETE = 'COMPLETE'
        ERROR = 'ERROR'
    
    # Sale categories
    class Category:
        HIGH = 'HIGH'
        MEDIUM = 'MEDIUM'
        LOW = 'LOW'
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal('0.05')
    DISCOUNT_RATE_TIER2 = Decimal('0.10')
    
    # Business rules - Tax rate
    TAX_RATE = Decimal('0.08')
    
    # Business rules - Cost ratio
    COST_RATIO = Decimal('0.60')
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD = Decimal('500.00')
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN = 'ETL'
    PREFIX_LOG_ID = 'LOG'
    PREFIX_ANALYTICS_ID = 'ANL'
    
    # Message texts
    MSG_INIT_SUCCESS = 'ETL process initialized successfully'
    MSG_EXTRACT_START = 'Starting data extraction'
    MSG_EXTRACT_COMPLETE = 'Data extraction completed'
    MSG_TRANSFORM_START = 'Starting data transformation'
    MSG_TRANSFORM_COMPLETE = 'Data transformation completed'
    MSG_LOAD_START = 'Starting data load'
    MSG_LOAD_COMPLETE = 'Data load completed'
    MSG_ETL_COMPLETE = 'ETL process completed successfully'
    MSG_ETL_ERROR = 'ETL process failed'


class SchemaDefinitions:
    """DataFrame schema definitions for ETL system."""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """Schema for raw sales data (ZSALES_RAW table)."""
        return StructType([
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
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    @staticmethod
    def analytics_schema() -> StructType:
        """Schema for analytics data (ZSALES_ANALYTICS table)."""
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
            StructField("loaded_at", TimestampType(), True),
            StructField("loaded_by", StringType(), True)
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        """Schema for ETL log data (ZETL_LOG table)."""
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", StringType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])


class TypeMappings:
    """Type mappings from ABAP to Python/PySpark."""
    
    ABAP_TO_PYSPARK = {
        'CHAR': StringType(),
        'NUMC': StringType(),
        'DATS': DateType(),
        'TIMS': StringType(),
        'DEC': DecimalType(16, 2),
        'INT4': IntegerType(),
        'CURR': DecimalType(16, 2),
        'QUAN': DecimalType(16, 3),
        'TIMESTAMPL': TimestampType(),
        'WAERS': StringType(),
        'SYUNAME': StringType()
    }
    
    @staticmethod
    def get_spark_type(abap_type: str, length: int = None, decimals: int = None):
        """Convert ABAP type to Spark type with parameters."""
        base_type = TypeMappings.ABAP_TO_PYSPARK.get(abap_type)
        
        if abap_type == 'DEC' and length and decimals:
            return DecimalType(length, decimals)
        elif abap_type == 'CHAR' and length:
            return StringType()
        
        return base_type or StringType()


def load_config_from_yaml(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """Load additional configuration from YAML file."""
    import yaml
    from pathlib import Path
    
    config_file = Path(config_path)
    if not config_file.exists():
        return {}
    
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


# Global configuration instance
config = ETLConfig()
schemas = SchemaDefinitions()
type_mappings = TypeMappings()