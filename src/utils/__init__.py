"""
ETL Utilities Package.
Replaces ABAP includes (ZETL_TOP, ZETL_MACROS).
"""
from .decorators import (
    log_etl_step,
    log_etl_statistics,
    validate_fields,
    handle_etl_error,
    retry_on_failure
)

from .helpers import (
    generate_unique_id,
    generate_uuid,
    calculate_percentage,
    format_currency,
    add_days_to_date,
    safe_divide,
    truncate_string,
    is_valid_date,
    coalesce
)

from .validators import (
    FieldValidator,
    validate_record,
    ValidationResult
)

from .common_types import (
    ETLStatistics,
    RawSalesRecord,
    AnalyticsRecord,
    ETLLogEntry,
    ETLConfig,
    StatusCodes,
    ProcessSteps,
    SaleCategories
)

from .global_context import (
    ETLContext,
    ContextManager
)

__all__ = [
    # Decorators
    'log_etl_step',
    'log_etl_statistics',
    'validate_fields',
    'handle_etl_error',
    'retry_on_failure',
    
    # Helpers
    'generate_unique_id',
    'generate_uuid',
    'calculate_percentage',
    'format_currency',
    'add_days_to_date',
    'safe_divide',
    'truncate_string',
    'is_valid_date',
    'coalesce',
    
    # Validators
    'FieldValidator',
    'validate_record',
    'ValidationResult',
    
    # Common Types
    'ETLStatistics',
    'RawSalesRecord',
    'AnalyticsRecord',
    'ETLLogEntry',
    'ETLConfig',
    'StatusCodes',
    'ProcessSteps',
    'SaleCategories',
    
    # Context
    'ETLContext',
    'ContextManager'
]