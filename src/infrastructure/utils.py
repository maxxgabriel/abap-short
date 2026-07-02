```python
from datetime import datetime
from typing import Any
from decimal import Decimal

def generate_unique_id(prefix: str) -> str:
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"{prefix}{timestamp}"

def validate_required_field(value: Any, field_name: str) -> bool:
    if value is None or (isinstance(value, str) and value.strip() == ''):
        raise ValueError(f"Required field '{field_name}' is missing or empty")
    return True

def validate_currency(currency: str) -> bool:
    valid_currencies = ['USD', 'EUR', 'GBP', 'JPY']
    if currency not in valid_currencies:
        raise ValueError(f"Invalid currency: {currency}")
    return True

def validate_positive_amount(amount: Decimal, field_name: str) -> bool:
    if amount <= 0:
        raise ValueError(f"{field_name} must be positive, got {amount}")
    return True

def calculate_percentage(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100

def format_timestamp(dt: datetime = None) -> str:
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%Y-%m-%d %H:%M:%S')
```