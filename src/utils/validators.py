"""
Field Validation Utilities
Provides reusable validation functions for ETL data quality checks.
"""

from typing import Any, Optional
from decimal import Decimal


def validate_field(value: Any, field_name: str = "field") -> bool:
    """
    Validate that a field is not empty/null.
    
    Args:
        value: Field value to validate
        field_name: Name of field (for error messages)
        
    Returns:
        True if valid, False if invalid
    """
    if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
        return False
    return True


def validate_mandatory_fields(record: dict, required_fields: list) -> tuple[bool, Optional[str]]:
    """
    Validate that all required fields are present and not empty.
    
    Args:
        record: Dictionary containing record data
        required_fields: List of required field names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    for field in required_fields:
        if field not in record:
            return False, f"Missing required field: {field}"
        
        if not validate_field(record[field], field):
            return False, f"Empty required field: {field}"
    
    return True, None


def validate_numeric_range(
    value: Any,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    field_name: str = "field"
) -> tuple[bool, Optional[str]]:
    """
    Validate numeric value is within range.
    
    Args:
        value: Numeric value to validate
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        field_name: Field name for error messages
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        num_value = float(value)
        
        if min_value is not None and num_value < min_value:
            return False, f"{field_name} must be >= {min_value}"
        
        if max_value is not None and num_value > max_value:
            return False, f"{field_name} must be <= {max_value}"
        
        return True, None
        
    except (ValueError, TypeError):
        return False, f"{field_name} must be numeric"


def validate_currency(currency_code: str) -> bool:
    """
    Validate currency code format (ISO 4217).
    
    Args:
        currency_code: 3-letter currency code
        
    Returns:
        True if valid format
    """
    if not currency_code or not isinstance(currency_code, str):
        return False
    
    if len(currency_code) != 3:
        return False
    
    if not currency_code.isalpha() or not currency_code.isupper():
        return False
    
    return True


def validate_date_range(start_date: str, end_date: str) -> tuple[bool, Optional[str]]:
    """
    Validate that start date is before end date.
    
    Args:
        start_date: Start date string
        end_date: End date string
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not start_date or not end_date:
        return False, "Start and end dates are required"
    
    if start_date > end_date:
        return False, "Start date must be before end date"
    
    return True, None


def validate_category(category: str, valid_categories: list) -> bool:
    """
    Validate category value against allowed list.
    
    Args:
        category: Category value to validate
        valid_categories: List of valid category values
        
    Returns:
        True if valid
    """
    return category in valid_categories


def validate_positive_amount(amount: Any, field_name: str = "amount") -> tuple[bool, Optional[str]]:
    """
    Validate that amount is positive.
    
    Args:
        amount: Amount value to validate
        field_name: Field name for error messages
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        num_amount = float(amount)
        if num_amount <= 0:
            return False, f"{field_name} must be positive"
        return True, None
    except (ValueError, TypeError):
        return False, f"{field_name} must be numeric"