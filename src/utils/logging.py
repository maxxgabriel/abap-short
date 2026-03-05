"""
ETL Logging Utilities
Converted from ZETL_MACROS ABAP macros to Python functions
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession


class ETLLogger:
    """
    ETL Logger class for logging messages, statistics, and tracking ETL execution.
    Replaces ABAP macros: log_etl_message, log_etl_statistics
    """
    
    def __init__(self, etl_run_id: str, spark: SparkSession):
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._setup_logger()
        
    def _setup_logger(self):
        """Configure logger with appropriate format and handlers"""
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        level: str = "INFO"
    ):
        """
        Log ETL message with timestamp.
        Replaces ABAP macro: log_etl_message
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            level: Python logging level
        """
        log_entry = {
            "etl_run_id": self.etl_run_id,
            "step": step,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        log_msg = f"[{step}] [{status}] {message}"
        
        if level == "ERROR" or status == "E":
            self.logger.error(log_msg)
        elif level == "WARNING" or status == "W":
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
        
        return log_entry
    
    def log_statistics(
        self,
        step: str,
        status: str,
        records_processed: int,
        records_success: int,
        records_error: int,
        message: str
    ) -> Dict[str, Any]:
        """
        Log ETL statistics with record counts.
        Replaces ABAP macro: log_etl_statistics
        
        Args:
            step: ETL process step
            status: Status code
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
            message: Log message
        
        Returns:
            Dictionary containing log entry
        """
        log_entry = {
            "etl_run_id": self.etl_run_id,
            "step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        stats_msg = (
            f"[{step}] [{status}] {message} - "
            f"Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error}"
        )
        
        if status == "E":
            self.logger.error(stats_msg)
        elif status == "W":
            self.logger.warning(stats_msg)
        else:
            self.logger.info(stats_msg)
        
        return log_entry


def validate_field(value: Any, field_name: str) -> bool:
    """
    Validate mandatory field is not empty/None.
    Replaces ABAP macro: validate_field
    
    Args:
        value: Field value to validate
        field_name: Name of the field for error reporting
    
    Returns:
        True if valid, False otherwise
    """
    if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
        return False
    return True


def calculate_percentage(numerator: float, denominator: float) -> float:
    """
    Calculate percentage safely with division by zero handling.
    Replaces ABAP macro: calculate_percentage
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
    
    Returns:
        Percentage value (0.0 if denominator is 0)
    """
    if denominator > 0:
        return (numerator / denominator) * 100.0
    return 0.0


def generate_unique_id(prefix: str) -> str:
    """
    Generate unique ID with prefix and timestamp.
    Replaces ABAP macro: generate_unique_id
    
    Args:
        prefix: Prefix for the ID (e.g., 'ETL', 'LOG', 'ANL')
    
    Returns:
        Unique ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"{prefix}{timestamp}"


def format_currency(amount: float, currency: str, decimals: int = 2) -> str:
    """
    Format currency amount with specified decimal places.
    Replaces ABAP macro: format_currency
    
    Args:
        amount: Amount to format
        currency: Currency code (e.g., 'USD', 'EUR')
        decimals: Number of decimal places
    
    Returns:
        Formatted currency string
    """
    return f"{currency} {amount:,.{decimals}f}"


class ETLErrorHandler:
    """
    Context manager for ETL error handling.
    Replaces ABAP macro: handle_etl_error
    """
    
    def __init__(self, logger: ETLLogger, step: str, operation: str):
        self.logger = logger
        self.step = step
        self.operation = operation
        self.success = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            error_msg = f"{self.operation}: {str(exc_val)}"
            self.logger.log_message(
                step=self.step,
                status="E",
                message=error_msg,
                level="ERROR"
            )
            self.success = False
            return True  # Suppress exception
        return False


def calculate_business_metrics(
    quantity: int,
    unit_price: float,
    discount_qty_tier1: int = 10,
    discount_qty_tier2: int = 15,
    discount_rate_tier1: float = 0.05,
    discount_rate_tier2: float = 0.10,
    tax_rate: float = 0.08,
    cost_ratio: float = 0.60
) -> Dict[str, float]:
    """
    Calculate business metrics: gross, discount, tax, net amounts and profit margin.
    Consolidates logic from ABAP transformation classes.
    
    Args:
        quantity: Quantity sold
        unit_price: Price per unit
        discount_qty_tier1: Quantity threshold for tier 1 discount
        discount_qty_tier2: Quantity threshold for tier 2 discount
        discount_rate_tier1: Discount rate for tier 1
        discount_rate_tier2: Discount rate for tier 2
        tax_rate: Tax rate to apply
        cost_ratio: Cost as ratio of unit price
    
    Returns:
        Dictionary with calculated metrics
    """
    # Calculate gross amount
    gross_amount = quantity * unit_price
    
    # Calculate discount based on quantity tiers
    if quantity > discount_qty_tier2:
        discount_amount = gross_amount * discount_rate_tier2
    elif quantity > discount_qty_tier1:
        discount_amount = gross_amount * discount_rate_tier1
    else:
        discount_amount = 0.0
    
    # Calculate tax on discounted amount
    tax_amount = (gross_amount - discount_amount) * tax_rate
    
    # Calculate net amount
    net_amount = gross_amount - discount_amount + tax_amount
    
    # Calculate cost and profit margin
    cost = quantity * unit_price * cost_ratio
    if net_amount > 0:
        profit_margin = ((net_amount - cost) / net_amount) * 100.0
    else:
        profit_margin = 0.0
    
    return {
        "gross_amount": round(gross_amount, 2),
        "discount_amount": round(discount_amount, 2),
        "tax_amount": round(tax_amount, 2),
        "net_amount": round(net_amount, 2),
        "cost": round(cost, 2),
        "profit_margin": round(profit_margin, 2)
    }


def categorize_sale(
    gross_amount: float,
    high_threshold: float = 2000.0,
    medium_threshold: float = 500.0
) -> str:
    """
    Categorize sale based on gross amount.
    
    Args:
        gross_amount: Gross sale amount
        high_threshold: Threshold for HIGH category
        medium_threshold: Threshold for MEDIUM category
    
    Returns:
        Category string: 'HIGH', 'MEDIUM', or 'LOW'
    """
    if gross_amount >= high_threshold:
        return "HIGH"
    elif gross_amount >= medium_threshold:
        return "MEDIUM"
    else:
        return "LOW"


def validate_analytics_record(record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate analytics record for required fields and business rules.
    Replaces ABAP validation logic using validate_field macro.
    
    Args:
        record: Dictionary containing analytics record fields
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = [
        "analytics_id",
        "customer_id",
        "product_id",
        "gross_amount",
        "currency",
        "category"
    ]
    
    # Check required fields
    for field in required_fields:
        if not validate_field(record.get(field), field):
            return False, f"Missing or invalid required field: {field}"
    
    # Validate gross amount is positive
    if record.get("gross_amount", 0) <= 0:
        return False, "Gross amount must be positive"
    
    # Validate category
    valid_categories = ["HIGH", "MEDIUM", "LOW"]
    if record.get("category") not in valid_categories:
        return False, f"Invalid category: {record.get('category')}"
    
    return True, None


def create_execution_summary(
    etl_run_id: str,
    start_time: datetime,
    end_time: datetime,
    extracted: int,
    transformed: int,
    loaded: int,
    errors: int
) -> Dict[str, Any]:
    """
    Create ETL execution summary with statistics and timing.
    
    Args:
        etl_run_id: Unique ETL run identifier
        start_time: Process start time
        end_time: Process end time
        extracted: Number of records extracted
        transformed: Number of records transformed
        loaded: Number of records loaded
        errors: Number of errors encountered
    
    Returns:
        Dictionary containing execution summary
    """
    duration = (end_time - start_time).total_seconds()
    
    return {
        "etl_run_id": etl_run_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": round(duration, 2),
        "records_extracted": extracted,
        "records_transformed": transformed,
        "records_loaded": loaded,
        "records_error": errors,
        "success_rate": calculate_percentage(loaded, extracted) if extracted > 0 else 0.0,
        "status": "SUCCESS" if errors == 0 else "COMPLETED_WITH_ERRORS"
    }