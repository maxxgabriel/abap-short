"""
Data extraction module for Sales ETL process.
Extracts raw sales data from source systems.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class DataExtractor:
    """Handles extraction of raw sales data from source systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the data extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema definition
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional override for source data path
            
        Returns:
            DataFrame containing raw sales data, or None on failure
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Determine source path
            data_source = source_path or self.config.get("source.raw_sales_path")
            
            # Read data based on source type
            if data_source.endswith('.parquet'):
                df = self._extract_from_parquet(data_source, from_date, to_date)
            elif data_source.endswith('.csv'):
                df = self._extract_from_csv(data_source, from_date, to_date)
            elif 'jdbc' in self.config.get("source.type", ""):
                df = self._extract_from_jdbc(from_date, to_date)
            else:
                raise ValueError(f"Unsupported source type: {data_source}")
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')  # Only new records
            )
            
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered
            
        except Exception as e:
            self.log.error(f"Extraction failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            return None
    
    def _extract_from_parquet(
        self, 
        path: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Extract data from Parquet files."""
        schema = self.get_raw_sales_schema()
        df = self.spark.read.schema(schema).parquet(path)
        return df
    
    def _extract_from_csv(
        self, 
        path: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Extract data from CSV files."""
        schema = self.get_raw_sales_schema()
        df = self.spark.read \
            .schema(schema) \
            .option("header", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(path)
        return df
    
    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """Extract data from JDBC source (database)."""
        jdbc_config = self.config.get("source.jdbc", {})
        
        query = f"""
            (SELECT trans_id, trans_date, customer_id, product_id, 
                    quantity, unit_price, currency, sales_rep, region, 
                    status, created_at, created_by
             FROM {jdbc_config.get('table', 'sales_raw')}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') AS sales_data
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        return df
    
    def create_sample_data(self, output_path: str) -> None:
        """
        Create sample data for testing.
        
        Args:
            output_path: Path to write sample data
        """
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", 
             "John Doe", "NORTH", "N", datetime.now(), "SYSTEM"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD",
             "John Doe", "EAST", "N", datetime.now(), "SYSTEM"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD",
             "Bob Wilson", "WEST", "N", datetime.now(), "SYSTEM"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD",
             "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
        ]
        
        schema = self.get_raw_sales_schema()
        df = self.spark.createDataFrame(sample_data, schema)
        
        df.write.mode("overwrite").parquet(output_path)
        self.log.info(f"Sample data written to {output_path}")