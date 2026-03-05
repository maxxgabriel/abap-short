"""
Data Extraction Module
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from typing import Dict, Any, Optional
from datetime import datetime

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import ExtractError


class SalesDataExtractor:
    """Handles extraction of raw sales data"""
    
    SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.statistics: Dict[str, Any] = {}
    
    def extract_data(
        self,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """
        Extract raw sales data
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            DataFrame with raw sales data
        
        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read from source (adapt based on actual source)
            source_path = self.config.paths.get('source_path')
            
            if source_path:
                df = self._read_from_file(source_path, from_date, to_date)
            else:
                # Read from database/table
                df = self._read_from_table(from_date, to_date)
            
            # Validate extracted data
            count = df.count()
            
            if count == 0:
                self.logger.log_message(
                    step='EXTRACT',
                    status='W',
                    message="No records found for specified date range"
                )
            
            self.statistics['records_extracted'] = count
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f"Extracted {count} records successfully",
                records_processed=count,
                records_success=count
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractError(f"Data extraction failed: {str(e)}", original_error=e)
    
    def _read_from_file(
        self,
        source_path: str,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """Read data from file source"""
        df = self.spark.read \
            .schema(self.SCHEMA) \
            .option("header", "true") \
            .csv(source_path)
        
        # Filter by date range
        df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date) &
            (df.status == 'N')
        )
        
        return df
    
    def _read_from_table(
        self,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """Read data from database table"""
        table_name = self.config.etl_config.get('source_table', 'zsales_raw')
        
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
            FROM {table_name}
            WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
                AND status = 'N'
        """
        
        # For demo, create sample data
        return self._create_sample_data()
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for demonstration"""
        data = [
            ("T000001", "2024-01-15", "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", "2024-01-15", "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", "2024-01-15", "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", "2024-01-15", "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", "2024-01-15", "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(data, schema=self.SCHEMA)
    
    def validate_prerequisites(self) -> bool:
        """
        Validate extraction prerequisites
        
        Returns:
            True if prerequisites are met
        """
        # Check if source is accessible
        source_path = self.config.paths.get('source_path')
        if source_path:
            # Validate file access (simplified)
            return True
        
        # Validate database connection (simplified)
        return True