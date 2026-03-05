"""
Field Validation Utilities

Provides validation functions for ETL data quality checks.
"""

from typing import Any, Optional
from decimal import Decimal


def validate_field(value: Any, field_name: str = "field") -> bool:
    """
    Validate that a field is not empty/null.
    
    Args:
        value: Value to validate
        field_name: Name of field for error messages
        
    Returns:
        True if valid, False otherwise
    """
    if value is None:
        return False
    
    if isinstance(value, str) and not value.strip():
        return False
    
    return True


def validate_mandatory_fields(record: dict, required_fields: list) -> tuple[bool, Optional[str]]:
    """
    Validate that all mandatory fields are present and non-empty.
    
    Args:
        record: Dictionary containing record data
        required_fields: List of required field names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    for field in required_fields:
        if field not in record or not validate_field(record[field], field):
            return False, f"Mandatory field '{field}' is missing or empty"
    
    return True, None


def validate_numeric_field(value: Any, field_name: str, min_value: Optional[float] = None) -> tuple[bool, Optional[str]]:
    """
    Validate numeric field.
    
    Args:
        value: Value to validate
        field_name: Field name
        min_value: Optional minimum value
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if value is None:
        return False, f"Field '{field_name}' is null"
    
    try:
        num_value = float(value)
        if min_value is not None and num_value < min_value:
            return False, f"Field '{field_name}' value {num_value} is below minimum {min_value}"
        return True, None
    except (ValueError, TypeError):
        return False, f"Field '{field_name}' is not a valid number"


def validate_currency_field(value: Any, currency: str) -> tuple[bool, Optional[str]]:
    """
    Validate currency field.
    
    Args:
        value: Currency value
        currency: Currency code
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check currency code
    if not validate_field(currency, "currency"):
        return False, "Currency code is missing"
    
    # Check amount
    is_valid, error = validate_numeric_field(value, "currency_amount", min_value=0)
    if not is_valid:
        return False, error
    
    return True, None


def validate_category(category: str, valid_categories: list) -> tuple[bool, Optional[str]]:
    """
    Validate category value.
    
    Args:
        category: Category value
        valid_categories: List of valid category values
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not validate_field(category, "category"):
        return False, "Category is missing"
    
    if category not in valid_categories:
        return False, f"Invalid category '{category}'. Must be one of {valid_categories}"
    
    return True, None


def validate_date_range(from_date: str, to_date: str) -> tuple[bool, Optional[str]]:
    """
    Validate date range.
    
    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    from datetime import datetime
    
    try:
        start = datetime.strptime(from_date, '%Y-%m-%d')
        end = datetime.strptime(to_date, '%Y-%m-%d')
        
        if start > end:
            return False, "From date cannot be later than to date"
        
        if end > datetime.now():
            return False, "To date cannot be in the future"
        
        return True, None
        
    except ValueError as e:
        return False, f"Invalid date format: {str(e)}"