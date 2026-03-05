"""
Data extraction component.

This module extracts raw sales data from the source system,
replacing the ABAP ZCL_ETL_EXTRACTOR class.
"""

from datetime import date
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.base_component import ETLComponentInterface, ExecutionResult
from src.constants import ProcessStep, Status
from src.exceptions import ExtractError
from src.logger import ETLLogger
from src.models import RawSalesData


class DataExtractor(ETLComponentInterface):
    """
    Extracts raw sales data from source system.
    
    Implements the extraction phase of the ETL pipeline using PySpark.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        logger: ETLLogger,
        source_table: str = "zsales_raw"
    ):
        """
        Initialize data extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger
            source_table: Name of source table
        """
        self.spark = spark
        self.logger = logger
        self.source_table = source_table
    
    def execute(self) -> ExecutionResult:
        """
        Execute extraction (placeholder implementation).
        
        Returns:
            ExecutionResult: Execution results
        """
        raise NotImplementedError("Use extract_data method instead")
    
    def get_component_name(self) -> str:
        """Get component name."""
        return "DataExtractor"
    
    def validate_prerequisites(self) -> bool:
        """
        Validate extraction prerequisites.
        
        Returns:
            bool: True if prerequisites are met
        """
        if self.spark is None:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=Status.ERROR,
                message="SparkSession not initialized"
            )
            return False
        
        return True
    
    def extract_data(
        self,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Extract sales data for the specified date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame: Extracted sales data
            
        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=Status.INFO,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Define schema
            schema = self._get_raw_sales_schema()
            
            # In production, this would read from actual source
            # For demonstration, create sample data
            sample_data = self._create_sample_data(from_date)
            
            # Create DataFrame
            df = self.spark.createDataFrame(sample_data, schema=schema)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == "N")
            )
            
            count = df_filtered.count()
            
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=Status.SUCCESS,
                message=f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=Status.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(f"Failed to extract data: {str(e)}")
    
    @staticmethod
    def _get_raw_sales_schema() -> StructType:
        """
        Get schema for raw sales data.
        
        Returns:
            StructType: Schema definition
        """
        return StructType([
            StructField("trans_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), True),
            StructField("region", StringType(), True),
            StructField("status", StringType(), False)
        ])
    
    @staticmethod
    def _create_sample_data(trans_date: date) -> List[tuple]:
        """
        Create sample data for demonstration.
        
        Args:
            trans_date: Transaction date
            
        Returns:
            List[tuple]: Sample data rows
        """
        from decimal import Decimal
        
        return [
            ("T000001", trans_date, "CUST001", "PROD001", 10, 
             Decimal("99.99"), "USD", "John Doe", "NORTH", "N"),
            ("T000002", trans_date, "CUST002", "PROD002", 5, 
             Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", trans_date, "CUST003", "PROD001", 20, 
             Decimal("99.99"), "USD", "John Doe", "EAST", "N"),
            ("T000004", trans_date, "CUST001", "PROD003", 3, 
             Decimal("299.99"), "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", trans_date, "CUST004", "PROD002", 15, 
             Decimal("149.99"), "USD", "Jane Smith", "SOUTH", "N"),
        ]