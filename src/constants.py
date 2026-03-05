"""
Constants and configuration values for ETL system.
"""


class ETLConstants:
    """ETL system constants."""
    
    # Status codes
    STATUS_NEW = "N"
    STATUS_PROCESSED = "P"
    STATUS_ERROR = "E"
    STATUS_WARNING = "W"
    STATUS_SUCCESS = "S"
    STATUS_INFO = "I"
    
    # ETL process steps
    STEP_INIT = "INIT"
    STEP_EXTRACT = "EXTRACT"
    STEP_TRANSFORM = "TRANSFORM"
    STEP_LOAD = "LOAD"
    STEP_VALIDATE = "VALIDATE"
    STEP_COMPLETE = "COMPLETE"
    STEP_ERROR = "ERROR"
    
    # Sale categories
    CATEGORY_HIGH = "HIGH"
    CATEGORY_MEDIUM = "MEDIUM"
    CATEGORY_LOW = "LOW"
    
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