"""Constants and enumerations for ETL system."""
from enum import Enum


class Status(str, Enum):
    """Status codes for ETL processing."""
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessStep(str, Enum):
    """ETL process steps."""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class Category(str, Enum):
    """Sales category classifications."""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


class IDPrefix(str, Enum):
    """ID prefixes for generated identifiers."""
    ETL_RUN = 'ETL'
    LOG = 'LOG'
    ANALYTICS = 'ANL'


# Message templates
class Messages:
    """Standard message templates."""
    INIT_SUCCESS = 'ETL process initialized successfully'
    EXTRACT_START = 'Starting data extraction'
    EXTRACT_COMPLETE = 'Data extraction completed'
    TRANSFORM_START = 'Starting data transformation'
    TRANSFORM_COMPLETE = 'Data transformation completed'
    LOAD_START = 'Starting data load'
    LOAD_COMPLETE = 'Data load completed'
    ETL_COMPLETE = 'ETL process completed successfully'
    ETL_ERROR = 'ETL process failed'