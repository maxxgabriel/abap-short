"""
ETL Extractor Component

Extracts raw sales data from source tables.
Converted from ABAP ZCL_ETL_EXTRACTOR class.
"""

from typing import Tuple
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

from src.schemas import ETLSchemas
from src.constants import ETLConstants
from src.logger import ETLLogger


class ETLExtractor:
    """Extracts raw sales data from source systems"""

    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.schema = ETLSchemas.raw_sales_schema()

    def extract_data(
        self,
        from_date: date,
        to_date: date,
        source_table: str
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for date range
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_table: Source table name/path
            
        Returns:
            Tuple of (DataFrame with extracted data, success flag)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.INFO,
                message=f"Starting extraction from {from_date} to {to_date}"
            )

            # Read from source table
            df = self._read_source_data(source_table, from_date, to_date)
            
            # Apply filters
            df = self._apply_filters(df)
            
            # Validate schema
            df = self._validate_schema(df)
            
            # Cache for performance
            df.cache()
            
            count = df.count()
            
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                message=f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count
            )
            
            return df, True

        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], self.schema), False

    def _read_source_data(
        self,
        source_table: str,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """Read data from source table with date filter"""
        return (
            self.spark.table(source_table)
            .filter(
                (col("trans_date") >= lit(from_date)) &
                (col("trans_date") <= lit(to_date))
            )
        )

    def _apply_filters(self, df: DataFrame) -> DataFrame:
        """Apply business filters to extracted data"""
        # Filter only new/unprocessed records
        df = df.filter(col("status") == ETLConstants.Status.NEW)
        
        # Filter out invalid records
        df = df.filter(
            col("trans_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("quantity") > 0) &
            (col("unit_price") > 0)
        )
        
        return df

    def _validate_schema(self, df: DataFrame) -> DataFrame:
        """Ensure DataFrame conforms to expected schema"""
        # Select only required columns in correct order
        expected_fields = [field.name for field in self.schema.fields]
        
        # Add missing columns with null values if needed
        for field in expected_fields:
            if field not in df.columns:
                df = df.withColumn(field, lit(None).cast(
                    next(f.dataType for f in self.schema.fields if f.name == field)
                ))
        
        # Select columns in schema order
        return df.select(*expected_fields)

    def create_sample_data(self, output_path: str) -> None:
        """
        Create sample raw sales data for testing
        
        Args:
            output_path: Path to write sample data
        """
        from datetime import datetime
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", 
             "John Doe", "NORTH", "N", datetime.now(), "system"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", datetime.now(), "system"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD",
             "John Doe", "EAST", "N", datetime.now(), "system"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD",
             "Bob Wilson", "WEST", "N", datetime.now(), "system"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", datetime.now(), "system"),
        ]
        
        df = self.spark.createDataFrame(sample_data, schema=self.schema)
        df.write.mode("overwrite").format("delta").save(output_path)
        
        self.logger.log_message(
            step=ETLConstants.Step.INIT,
            status=ETLConstants.Status.SUCCESS,
            message=f"Sample data created at {output_path}",
            records_processed=len(sample_data)
        )