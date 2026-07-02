"""
Calculation Utilities
Reusable calculation functions for ETL transformations.
"""

from typing import Optional
from decimal import Decimal, ROUND_HALF_UP


def calculate_percentage(numerator: float, denominator: float, decimals: int = 2) -> float:
    """
    Calculate percentage with safe division.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        decimals: Decimal places for rounding
        
    Returns:
        Percentage value (0 if denominator is 0)
    """
    if denominator == 0 or denominator is None:
        return 0.0
    
    percentage = (numerator / denominator) * 100
    return round(percentage, decimals)


def calculate_discount(gross_amount: float, quantity: int, tier1_qty: int = 10, 
                       tier2_qty: int = 15, tier1_rate: float = 0.05, 
                       tier2_rate: float = 0.10) -> float:
    """
    Calculate tiered discount based on quantity.
    
    Args:
        gross_amount: Gross amount before discount
        quantity: Quantity purchased
        tier1_qty: Quantity threshold for tier 1 discount
        tier2_qty: Quantity threshold for tier 2 discount
        tier1_rate: Discount rate for tier 1
        tier2_rate: Discount rate for tier 2
        
    Returns:
        Discount amount
    """
    if quantity > tier2_qty:
        return round(gross_amount * tier2_rate, 2)
    elif quantity > tier1_qty:
        return round(gross_amount * tier1_rate, 2)
    else:
        return 0.0


def calculate_tax(taxable_amount: float, tax_rate: float = 0.08) -> float:
    """
    Calculate tax amount.
    
    Args:
        taxable_amount: Amount subject to tax
        tax_rate: Tax rate (default 8%)
        
    Returns:
        Tax amount
    """
    if taxable_amount <= 0:
        return 0.0
    
    return round(taxable_amount * tax_rate, 2)


def calculate_net_amount(gross_amount: float, discount: float, tax: float) -> float:
    """
    Calculate net amount after discount and tax.
    
    Args:
        gross_amount: Gross amount
        discount: Discount amount
        tax: Tax amount
        
    Returns:
        Net amount
    """
    net = gross_amount - discount + tax
    return round(net, 2)


def calculate_profit_margin(revenue: float, cost: float) -> float:
    """
    Calculate profit margin percentage.
    
    Args:
        revenue: Revenue amount
        cost: Cost amount
        
    Returns:
        Profit margin percentage
    """
    if revenue <= 0:
        return 0.0
    
    profit = revenue - cost
    margin = (profit / revenue) * 100
    return round(margin, 2)


def categorize_sale(gross_amount: float, high_threshold: float = 2000.0, 
                    medium_threshold: float = 500.0) -> str:
    """
    Categorize sale based on gross amount.
    
    Args:
        gross_amount: Gross sale amount
        high_threshold: Threshold for HIGH category
        medium_threshold: Threshold for MEDIUM category
        
    Returns:
        Category string (HIGH, MEDIUM, or LOW)
    """
    if gross_amount >= high_threshold:
        return "HIGH"
    elif gross_amount >= medium_threshold:
        return "MEDIUM"
    else:
        return "LOW"


def format_currency(amount: float, currency: str, decimals: int = 2) -> str:
    """
    Format amount as currency string.
    
    Args:
        amount: Amount to format
        currency: Currency code
        decimals: Decimal places
        
    Returns:
        Formatted currency string
    """
    formatted = f"{amount:,.{decimals}f}"
    return f"{formatted} {currency}"