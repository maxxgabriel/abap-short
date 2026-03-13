"""
ETL Extractor Module
Extracts raw sales data from source
"""
from datetime import date
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.constants import constants
from src.exceptions import ETLExtractionError


class ETLExtractor:
    """Extracts raw sales data from source tables"""
    
    # Define schema for raw sales data
    RAW_SALES_SCHEMA = StructType([
        StructField("trans_id", StringType(), False),
        StructField("trans_date", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), False)
    ])
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the extractor
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
        """
        self.spark = spark
        self.logger = logger
    
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        source_table: str = "sales_raw",
        status_filter: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data from source
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_table: Source table name
            status_filter: Optional status filter (default: 'N' for new)
            
        Returns:
            DataFrame containing raw sales data
            
        Raises:
            ETLExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step=constants.STEP.EXTRACT,
                status=constants.STATUS.INFO,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Build query
            query = f"""
                SELECT 
                    trans_id,
                    trans_date,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price,
                    currency,
                    sales_rep,
                    region,
                    status
                FROM {source_table}
                WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
            """
            
            if status_filter:
                query += f" AND status = '{status_filter}'"
            else:
                query += f" AND status = '{constants.STATUS.NEW}'"
            
            # Execute extraction
            df = self.spark.sql(query)
            
            # Get count
            record_count = df.count()
            
            # Log success
            self.logger.log_message(
                step=constants.STEP.EXTRACT,
                status=constants.STATUS.SUCCESS,
                message=f"Extracted {record_count} records successfully",
                records_processed=record_count,
                records_success=record_count
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step=constants.STEP.EXTRACT,
                status=constants.STATUS.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            raise ETLExtractionError(
                message=f"Failed to extract data from {source_table}",
                original_exception=e
            )
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample raw sales data for testing
        
        Returns:
            DataFrame with sample data
        """
        sample_data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.RAW_SALES_SCHEMA)
        
        record_count = df.count()
        self.logger.log_message(
            step=constants.STEP.EXTRACT,
            status=constants.STATUS.SUCCESS,
            message=f"Created {record_count} sample records",
            records_processed=record_count,
            records_success=record_count
        )
        
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check if DataFrame is empty
            if df.count() == 0:
                self.logger.log_message(
                    step=constants.STEP.VALIDATE,
                    status=constants.STATUS.WARNING,
                    message="No records extracted"
                )
                return False
            
            # Check for required columns
            required_columns = [field.name for field in self.RAW_SALES_SCHEMA.fields]
            missing_columns = set(required_columns) - set(df.columns)
            
            if missing_columns:
                self.logger.log_message(
                    step=constants.STEP.VALIDATE,
                    status=constants.STATUS.ERROR,
                    message=f"Missing required columns: {missing_columns}"
                )
                return False
            
            # Check for nulls in required fields
            null_counts = df.select([
                (df[col].isNull().cast("int").alias(col))
                for col in required_columns
            ]).agg(*[f"sum({col}) as {col}" for col in required_columns]).collect()[0]
            
            has_nulls = any(null_counts[col] > 0 for col in required_columns)
            if has_nulls:
                self.logger.log_message(
                    step=constants.STEP.VALIDATE,
                    status=constants.STATUS.WARNING,
                    message="Found null values in required fields"
                )
                return False
            
            self.logger.log_message(
                step=constants.STEP.VALIDATE,
                status=constants.STATUS.SUCCESS,
                message="Extracted data validation passed"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=constants.STEP.VALIDATE,
                status=constants.STATUS.ERROR,
                message=f"Validation failed: {str(e)}"
            )
            return False