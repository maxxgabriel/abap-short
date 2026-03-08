from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from typing import Tuple
from src.logger import ETLLogger
from src.constants import ProcessStep, ProcessStatus
from src.exceptions import ExtractError
from src.models import SchemaDefinitions


class DataExtractor:
    """Extracts raw sales data from source"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        self.spark = spark
        self.logger = logger
        self.schema = SchemaDefinitions.raw_sales_schema()
    
    def extract_data(
        self,
        from_date: datetime,
        to_date: datetime,
        source_path: str = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract sales data within date range
        
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=ProcessStatus.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            if source_path:
                # Read from file/table
                df = self.spark.read.schema(self.schema).parquet(source_path)
                df = df.filter(
                    (df.trans_date >= from_date) &
                    (df.trans_date <= to_date) &
                    (df.status == 'N')
                )
            else:
                # Create sample data for testing
                df = self._create_sample_data(from_date, to_date)
            
            count = df.count()
            
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=ProcessStatus.SUCCESS,
                message=f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count
            )
            
            return df, True
            
        except Exception as e:
            self.logger.log_message(
                step=ProcessStep.EXTRACT,
                status=ProcessStatus.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(
                message=f"Failed to extract data: {str(e)}",
                step=ProcessStep.EXTRACT
            )
    
    def _create_sample_data(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> DataFrame:
        """Create sample sales data for testing"""
        
        sample_data = [
            ("T000001", from_date, "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", from_date, "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", from_date, "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", from_date, "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", from_date, "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(sample_data, self.schema)