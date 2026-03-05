"""
ETL Validation Utility Module
Provides field validation and data quality checks for ETL processes.
"""

from typing import Any, List, Optional, Callable
from decimal import Decimal
from datetime import datetime


class ValidationResult:
    """Result of a validation check."""
    
    def __init__(self, is_valid: bool = True, errors: Optional[List[str]] = None):
        """
        Initialize validation result.
        
        Args:
            is_valid: Whether validation passed
            errors: List of validation error messages
        """
        self.is_valid = is_valid
        self.errors = errors or []
    
    def add_error(self, error: str):
        """Add a validation error."""
        self.is_valid = False
        self.errors.append(error)
    
    def __bool__(self):
        """Boolean evaluation returns validation status."""
        return self.is_valid
    
    def __str__(self):
        """String representation."""
        if self.is_valid:
            return "Validation passed"
        return f"Validation failed: {'; '.join(self.errors)}"


class FieldValidator:
    """
    Field validation utility class.
    Provides validation functions equivalent to ABAP validate_field macro.
    """
    
    @staticmethod
    def validate_not_empty(value: Any, field_name: str = "field") -> ValidationResult:
        """
        Validate that a field is not empty or None.
        
        Args:
            value: Value to validate
            field_name: Name of field for error messages
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        if value is None:
            result.add_error(f"{field_name} cannot be None")
        elif isinstance(value, str) and not value.strip():
            result.add_error(f"{field_name} cannot be empty")
        elif isinstance(value, (list, dict)) and len(value) == 0:
            result.add_error(f"{field_name} cannot be empty")
            
        return result
    
    @staticmethod
    def validate_positive(value: float, field_name: str = "field") -> ValidationResult:
        """
        Validate that a numeric value is positive.
        
        Args:
            value: Value to validate
            field_name: Name of field for error messages
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        if value is None:
            result.add_error(f"{field_name} cannot be None")
        elif value <= 0:
            result.add_error(f"{field_name} must be positive (got {value})")
            
        return result
    
    @staticmethod
    def validate_in_list(value: Any, valid_values: List[Any], field_name: str = "field") -> ValidationResult:
        """
        Validate that a value is in a list of valid values.
        
        Args:
            value: Value to validate
            valid_values: List of valid values
            field_name: Name of field for error messages
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        if value not in valid_values:
            result.add_error(f"{field_name} must be one of {valid_values} (got {value})")
            
        return result
    
    @staticmethod
    def validate_date_range(
        date_value: datetime,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
        field_name: str = "date"
    ) -> ValidationResult:
        """
        Validate that a date is within a range.
        
        Args:
            date_value: Date to validate
            min_date: Minimum allowed date
            max_date: Maximum allowed date
            field_name: Name of field for error messages
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        if date_value is None:
            result.add_error(f"{field_name} cannot be None")
            return result
        
        if min_date and date_value < min_date:
            result.add_error(f"{field_name} must be >= {min_date}")
        
        if max_date and date_value > max_date:
            result.add_error(f"{field_name} must be <= {max_date}")
            
        return result
    
    @staticmethod
    def validate_currency_code(currency: str) -> ValidationResult:
        """
        Validate currency code format.
        
        Args:
            currency: Currency code to validate
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        if not currency or len(currency) != 3:
            result.add_error(f"Currency must be 3-character code (got {currency})")
        elif not currency.isalpha():
            result.add_error(f"Currency must be alphabetic (got {currency})")
        elif not currency.isupper():
            result.add_error(f"Currency must be uppercase (got {currency})")
            
        return result
    
    @staticmethod
    def validate_custom(
        value: Any,
        validator_func: Callable[[Any], bool],
        error_message: str
    ) -> ValidationResult:
        """
        Validate using a custom validation function.
        
        Args:
            value: Value to validate
            validator_func: Function that returns True if valid
            error_message: Error message if validation fails
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        try:
            if not validator_func(value):
                result.add_error(error_message)
        except Exception as e:
            result.add_error(f"Validation error: {str(e)}")
            
        return result


class RecordValidator:
    """
    Record-level validation for ETL data structures.
    Provides validation equivalent to ABAP loader validation methods.
    """
    
    def __init__(self):
        """Initialize record validator."""
        self.field_validator = FieldValidator()
    
    def validate_analytics_record(self, record: dict) -> ValidationResult:
        """
        Validate an analytics record.
        
        Args:
            record: Analytics record dictionary
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        # Validate required fields
        required_fields = [
            'analytics_id', 'customer_id', 'product_id', 
            'gross_amount', 'net_amount', 'currency', 'category'
        ]
        
        for field in required_fields:
            field_result = self.field_validator.validate_not_empty(
                record.get(field), field
            )
            if not field_result:
                result.errors.extend(field_result.errors)
                result.is_valid = False
        
        # Validate amounts are positive
        if 'gross_amount' in record:
            amount_result = self.field_validator.validate_positive(
                record['gross_amount'], 'gross_amount'
            )
            if not amount_result:
                result.errors.extend(amount_result.errors)
                result.is_valid = False
        
        # Validate currency code
        if 'currency' in record:
            currency_result = self.field_validator.validate_currency_code(
                record['currency']
            )
            if not currency_result:
                result.errors.extend(currency_result.errors)
                result.is_valid = False
        
        # Validate category
        if 'category' in record:
            category_result = self.field_validator.validate_in_list(
                record['category'], ['HIGH', 'MEDIUM', 'LOW'], 'category'
            )
            if not category_result:
                result.errors.extend(category_result.errors)
                result.is_valid = False
        
        return result
    
    def validate_raw_sales_record(self, record: dict) -> ValidationResult:
        """
        Validate a raw sales record.
        
        Args:
            record: Raw sales record dictionary
            
        Returns:
            ValidationResult instance
        """
        result = ValidationResult()
        
        # Validate required fields
        required_fields = [
            'trans_id', 'trans_date', 'customer_id', 'product_id',
            'quantity', 'unit_price', 'currency'
        ]
        
        for field in required_fields:
            field_result = self.field_validator.validate_not_empty(
                record.get(field), field
            )
            if not field_result:
                result.errors.extend(field_result.errors)
                result.is_valid = False
        
        # Validate numeric fields are positive
        numeric_fields = ['quantity', 'unit_price']
        for field in numeric_fields:
            if field in record:
                numeric_result = self.field_validator.validate_positive(
                    record[field], field
                )
                if not numeric_result:
                    result.errors.extend(numeric_result.errors)
                    result.is_valid = False
        
        # Validate currency
        if 'currency' in record:
            currency_result = self.field_validator.validate_currency_code(
                record['currency']
            )
            if not currency_result:
                result.errors.extend(currency_result.errors)
                result.is_valid = False
        
        return result


def validate_batch(
    records: List[dict],
    validator_func: Callable[[dict], ValidationResult]
) -> tuple:
    """
    Validate a batch of records.
    
    Args:
        records: List of records to validate
        validator_func: Function to validate each record
        
    Returns:
        Tuple of (valid_records, invalid_records, error_messages)
    """
    valid_records = []
    invalid_records = []
    error_messages = []
    
    for idx, record in enumerate(records):
        result = validator_func(record)
        if result.is_valid:
            valid_records.append(record)
        else:
            invalid_records.append(record)
            error_messages.append(f"Record {idx}: {'; '.join(result.errors)}")
    
    return valid_records, invalid_records, error_messages