"""
Exception classes for ETL system.
Converted from ZCX_ETL_ERROR.
"""


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(
        self,
        message: str,
        error_step: str = "",
        record_id: str = "",
        original_error: Exception = None
    ):
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.original_error = original_error
        super().__init__(self.message)
    
    def __str__(self):
        parts = [self.message]
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        if self.original_error:
            parts.append(f"Original: {str(self.original_error)}")
        return " | ".join(parts)


class ExtractError(ETLError):
    """Exception for extraction errors."""
    pass


class TransformError(ETLError):
    """Exception for transformation errors."""
    pass


class LoadError(ETLError):
    """Exception for loading errors."""
    pass


class ValidationError(ETLError):
    """Exception for validation errors."""
    pass


class ConfigurationError(ETLError):
    """Exception for configuration errors."""
    pass