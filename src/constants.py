"""
ETL system constants and configuration.
Converted from ABAP ZCL_ETL_CONSTANTS.
"""

from decimal import Decimal


class ETLConstants:
    """Constants and configuration for ETL system."""

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
    DISCOUNT_RATE_TIER1 = Decimal("0.05")
    DISCOUNT_RATE_TIER2 = Decimal("0.10")

    # Business rules - Tax rate
    TAX_RATE = Decimal("0.08")

    # Business rules - Cost ratio
    COST_RATIO = Decimal("0.60")

    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD = Decimal("500.00")

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