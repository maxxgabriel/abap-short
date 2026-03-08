"""
Extractor Module
Handles data extraction from source systems.
"""
from datetime import date
from typing import List, Tuple
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.types import RawSales
from src.constants import ETLStep, ETLStatus
from src.exceptions import ExtractError


class ETLExtractor:
    """Extracts raw sales data from source."""
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize extractor.
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark
    
    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_table: str = "zsales_raw"
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name
            
        Returns:
            Tuple of (DataFrame, success_flag)
            
        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Define schema
            schema = self._get_raw_sales_schema()
            
            # Extract data (simulated with sample data for demo)
            # In production: df = self.spark.read.table(source_table).filter(...)
            df = self._create_sample_data(schema, from_date)
            
            count = df.count()
            
            self.logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.SUCCESS,
                message=f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count
            )
            
            return df, True
            
        except Exception as e:
            self.logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(f"Failed to extract data: {str(e)}", previous=e)
    
    def _get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data."""
        return StructType([
            StructField("trans_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), False),
            StructField("region", StringType(), False),
            StructField("status", StringType(), False)
        ])
    
    def _create_sample_data(self, schema: StructType, trans_date: date) -> DataFrame:
        """Create sample data for demonstration."""
        from pyspark.sql import Row
        
        sample_data = [
            Row(
                trans_id="T000001",
                trans_date=trans_date,
                customer_id="CUST001",
                product_id="PROD001",
                quantity=10,
                unit_price=Decimal("99.99"),
                currency="USD",
                sales_rep="John Doe",
                region="NORTH",
                status="N"
            ),
            Row(
                trans_id="T000002",
                trans_date=trans_date,
                customer_id="CUST002",
                product_id="PROD002",
                quantity=5,
                unit_price=Decimal("149.99"),
                currency="USD",
                sales_rep="Jane Smith",
                region="SOUTH",
                status="N"
            ),
            Row(
                trans_id="T000003",
                trans_date=trans_date,
                customer_id="CUST003",
                product_id="PROD001",
                quantity=20,
                unit_price=Decimal("99.99"),
                currency="USD",
                sales_rep="John Doe",
                region="EAST",
                status="N"
            ),
            Row(
                trans_id="T000004",
                trans_date=trans_date,
                customer_id="CUST001",
                product_id="PROD003",
                quantity=3,
                unit_price=Decimal("299.99"),
                currency="USD",
                sales_rep="Bob Wilson",
                region="WEST",
                status="N"
            ),
            Row(
                trans_id="T000005",
                trans_date=trans_date,
                customer_id="CUST004",
                product_id="PROD002",
                quantity=15,
                unit_price=Decimal("149.99"),
                currency="USD",
                sales_rep="Jane Smith",
                region="SOUTH",
                status="N"
            )
        ]
        
        return self.spark.createDataFrame(sample_data, schema)