"""
Integration Tests for Configuration

Tests configuration integration with ETL components.
"""

import pytest
from decimal import Decimal
from pyspark.sql import SparkSession

from src.config_loader import ConfigLoader
from src.constants import SaleCategory


class TestConfigurationIntegration:
    """Integration tests for configuration usage"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session"""
        spark = (SparkSession.builder
                .master("local[2]")
                .appName("test_config")
                .getOrCreate())
        
        yield spark
        
        spark.stop()
    
    @pytest.fixture
    def config(self):
        """Load configuration"""
        return ConfigLoader("config.yaml")
    
    def test_discount_tier_logic(self, config):
        """Test discount tier business logic"""
        # Quantity below tier 1
        quantity = 5
        assert quantity <= config.discount_qty_tier1
        discount_rate = Decimal('0')
        
        # Quantity in tier 1
        quantity = 12
        if quantity > config.discount_qty_tier1:
            discount_rate = config.discount_rate_tier1
        assert discount_rate == Decimal('0.05')
        
        # Quantity in tier 2
        quantity = 20
        if quantity > config.discount_qty_tier2:
            discount_rate = config.discount_rate_tier2
        assert discount_rate == Decimal('0.10')
    
    def test_category_classification(self, config):
        """Test category classification logic"""
        # High category
        gross_amount = Decimal('2500.00')
        if gross_amount >= config.category_high_threshold:
            category = SaleCategory.HIGH
        elif gross_amount >= config.category_medium_threshold:
            category = SaleCategory.MEDIUM
        else:
            category = SaleCategory.LOW
        
        assert category == SaleCategory.HIGH
        
        # Medium category
        gross_amount = Decimal('750.00')
        if gross_amount >= config.category_high_threshold:
            category = SaleCategory.HIGH
        elif gross_amount >= config.category_medium_threshold:
            category = SaleCategory.MEDIUM
        else:
            category = SaleCategory.LOW
        
        assert category == SaleCategory.MEDIUM
        
        # Low category
        gross_amount = Decimal('250.00')
        if gross_amount >= config.category_high_threshold:
            category = SaleCategory.HIGH
        elif gross_amount >= config.category_medium_threshold:
            category = SaleCategory.MEDIUM
        else:
            category = SaleCategory.LOW
        
        assert category == SaleCategory.LOW
    
    def test_full_calculation_pipeline(self, config):
        """Test full calculation using configuration"""
        # Input values
        quantity = 15
        unit_price = Decimal('99.99')
        
        # Calculate gross
        gross = Decimal(str(quantity)) * unit_price
        assert gross == Decimal('1499.85')
        
        # Calculate discount
        if quantity > config.discount_qty_tier2:
            discount_rate = config.discount_rate_tier2
        elif quantity > config.discount_qty_tier1:
            discount_rate = config.discount_rate_tier1
        else:
            discount_rate = Decimal('0')
        
        discount = gross * discount_rate
        assert discount == Decimal('74.9925')
        
        # Calculate tax
        taxable = gross - discount
        tax = taxable * config.tax_rate
        
        # Calculate net
        net = taxable + tax
        
        # Verify final values
        assert net > gross  # Net includes tax
        assert discount > Decimal('0')  # Discount applied
    
    def test_spark_decimal_compatibility(self, spark, config):
        """Test Spark compatibility with Decimal configuration"""
        from pyspark.sql.types import DecimalType
        
        # Create sample data
        data = [(1, Decimal('100.00'))]
        df = spark.createDataFrame(data, ["id", "amount"])
        
        # Apply tax rate from config
        from pyspark.sql.functions import col, lit
        
        tax_rate_lit = lit(float(config.tax_rate))
        result = df.withColumn("tax", col("amount") * tax_rate_lit)
        
        # Verify calculation
        row = result.first()
        expected_tax = Decimal('100.00') * config.tax_rate
        
        # Allow small floating point difference
        assert abs(row["tax"] - float(expected_tax)) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])