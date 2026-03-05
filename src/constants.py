"""
ETL Constants and configuration values.
Migrated from ZCL_ETL_CONSTANTS ABAP class.
"""

from typing import Dict
import yaml
from pathlib import Path


class ETLConstants:
    """Constants for ETL system (from ZCL_ETL_CONSTANTS)"""

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

    @classmethod
    def load_config(cls, config_path: str = "config.yaml") -> Dict:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, "r") as f:
                return yaml.safe_load(f)
        return {}


# Create singleton instances for easy access
STATUS = ETLConstants.Status()
STEP = ETLConstants.Step()
CATEGORY = ETLConstants.Category()