"""
ETL Constants and Enumerations
Migrated from ABAP ZCL_ETL_CONSTANTS class
"""
from enum import Enum
from decimal import Decimal


class StatusCode(str, Enum):
    """ETL Status Codes"""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(str, Enum):
    """ETL Process Steps"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class SaleCategory(str, Enum):
    """Sale Category Classifications"""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


class ETLConstants:
    """ETL System Constants"""
    
    # Business Rules - Discount Thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal('0.05')
    DISCOUNT_RATE_TIER2 = Decimal('0.10')
    
    # Business Rules - Tax Rate
    TAX_RATE = Decimal('0.08')
    
    # Business Rules - Cost Ratio
    COST_RATIO = Decimal('0.60')
    
    # Business Rules - Category Thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD = Decimal('500.00')
    
    # ETL Configuration Defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID Prefixes
    PREFIX_ETL_RUN = 'ETL'
    PREFIX_LOG_ID = 'LOG'
    PREFIX_ANALYTICS_ID = 'ANL'
    
    # Message Templates
    MSG_INIT_SUCCESS = 'ETL process initialized successfully'
    MSG_EXTRACT_START = 'Starting data extraction'
    MSG_EXTRACT_COMPLETE = 'Data extraction completed'
    MSG_TRANSFORM_START = 'Starting data transformation'
    MSG_TRANSFORM_COMPLETE = 'Data transformation completed'
    MSG_LOAD_START = 'Starting data load'
    MSG_LOAD_COMPLETE = 'Data load completed'
    MSG_ETL_COMPLETE = 'ETL process completed successfully'
    MSG_ETL_ERROR = 'ETL process failed'


class IDGenerator:
    """Utility class for generating unique IDs"""
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """Generate unique ETL run ID"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"{ETLConstants.PREFIX_ETL_RUN}{timestamp}"
    
    @staticmethod
    def generate_log_id() -> str:
        """Generate unique log ID"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"{ETLConstants.PREFIX_LOG_ID}{timestamp}"
    
    @staticmethod
    def generate_analytics_id(trans_id: str) -> str:
        """Generate unique analytics ID"""
        from datetime import datetime
        timestamp = datetime.now().strftime('%H%M%S')
        return f"{ETLConstants.PREFIX_ANALYTICS_ID}{trans_id}{timestamp}"