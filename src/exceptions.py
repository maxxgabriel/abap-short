"""
ETL Exception Module
Custom exception classes for ETL errors
Migrated from ZCX_ETL_ERROR ABAP exception class
"""


class ETLException(Exception):
    """
    Base exception class for ETL errors.
    Migrates ABAP exception class to Python exception hierarchy.
    """

    def __init__(
        self,
        error_text: str = "",
        error_step: str = "",
        record_id: str = ""
    ):
        """
        Initialize ETL exception.
        
        Args:
            error_text: Error message text
            error_step: ETL step where error occurred
            record_id: Record ID related to error (if applicable)
        """
        self.error_text = error_text
        self.error_step = error_step
        self.record_id = record_id

        # Construct full error message
        message_parts = []
        if error_step:
            message_parts.append(f"[{error_step}]")
        if record_id:
            message_parts.append(f"Record {record_id}:")
        message_parts.append(error_text)

        super().__init__(" ".join(message_parts))


class ETLExtractException(ETLException):
    """Exception raised during data extraction."""
    pass


class ETLTransformException(ETLException):
    """Exception raised during data transformation."""
    pass


class ETLLoadException(ETLException):
    """Exception raised during data loading."""
    pass