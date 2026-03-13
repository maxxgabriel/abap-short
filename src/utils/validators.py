"""
Field validation utilities.
Replaces ABAP validate_field macro functionality.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ValidationResult:
    """Result of validation operation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]


class FieldValidator:
    """
    Field validation utility class.
    Replaces ABAP validate_field macro with OOP approach.
    """
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_required(self, value: Any, field_name: str) -> bool:
        """Validate required field is not empty"""
        if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
            self.errors.append(f"Field '{field_name}' is required")
            return False
        return True
    
    def validate_numeric(self, value: Any, field_name: str, 
                        min_val: Optional[float] = None,
                        max_val: Optional[float] = None) -> bool:
        """Validate numeric field"""
        try:
            num_val = float(value)
            
            if min_val is not None and num_val < min_val:
                self.errors.append(f"Field '{field_name}' must be >= {min_val}")
                return False
            
            if max_val is not None and num_val > max_val:
                self.errors.append(f"Field '{field_name}' must be <= {max_val}")
                return False
            
            return True
            
        except (ValueError, TypeError):
            self.errors.append(f"Field '{field_name}' must be numeric")
            return False
    
    def validate_date(self, value: Any, field_name: str,
                     min_date: Optional[datetime] = None,
                     max_date: Optional[datetime] = None) -> bool:
        """Validate date field"""
        if not isinstance(value, datetime):
            try:
                date_val = datetime.strptime(str(value), "%Y-%m-%d")
            except ValueError:
                self.errors.append(f"Field '{field_name}' is not a valid date")
                return False
        else:
            date_val = value
        
        if min_date and date_val < min_date:
            self.errors.append(f"Field '{field_name}' must be >= {min_date}")
            return False
        
        if max_date and date_val > max_date:
            self.errors.append(f"Field '{field_name}' must be <= {max_date}")
            return False
        
        return True
    
    def validate_length(self, value: str, field_name: str,
                       min_len: Optional[int] = None,
                       max_len: Optional[int] = None) -> bool:
        """Validate string length"""
        value_str = str(value) if value else ""
        length = len(value_str)
        
        if min_len is not None and length < min_len:
            self.errors.append(f"Field '{field_name}' must be at least {min_len} characters")
            return False
        
        if max_len is not None and length > max_len:
            self.errors.append(f"Field '{field_name}' must be at most {max_len} characters")
            return False
        
        return True
    
    def validate_in_list(self, value: Any, field_name: str, valid_values: List[Any]) -> bool:
        """Validate value is in allowed list"""
        if value not in valid_values:
            self.errors.append(
                f"Field '{field_name}' must be one of: {', '.join(map(str, valid_values))}"
            )
            return False
        return True
    
    def validate_currency(self, value: Any, field_name: str) -> bool:
        """Validate currency code"""
        valid_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
        return self.validate_in_list(value, field_name, valid_currencies)
    
    def get_validation_result(self) -> ValidationResult:
        """Get validation result"""
        return ValidationResult(
            is_valid=len(self.errors) == 0,
            errors=self.errors.copy(),
            warnings=self.warnings.copy()
        )
    
    def reset(self):
        """Reset validation state"""
        self.errors.clear()
        self.warnings.clear()


def validate_record(record: Dict[str, Any], required_fields: List[str]) -> ValidationResult:
    """
    Validate record has required fields.
    
    Args:
        record: Record dictionary
        required_fields: List of required field names
        
    Returns:
        ValidationResult object
    """
    validator = FieldValidator()
    
    for field in required_fields:
        validator.validate_required(record.get(field), field)
    
    return validator.get_validation_result()