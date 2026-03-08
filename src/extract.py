"""
Data extraction module for ETL system.
Converted from ABAP ZCL_ETL_EXTRACTOR class.
"""
from typing import Optional, Tuple
from datetime import date, datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

from src.schemas import ETLSchemas
from src.constants import ETLConstants
from src.logger import ETLLogger


class ETLExtractor:
    """
    Extracts raw sales data from source.
    
    Converted from ABAP ZCL_ETL_EXTRACTOR.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the extractor.
        
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
        to_date: date
    ) -> Tuple[Optional[DataFrame], bool]:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            Tuple of (DataFrame or None, success flag)
        """
        try:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Read data from source
            df = self._read_from_source()
            
            if df is None:
                raise ValueError("Failed to read data from source")
            
            # Filter by date range and status
            df_filtered = df.filter(
                (col("trans_date") >= lit(from_date)) &
                (col("trans_date") <= lit(to_date)) &
                (col("status") == lit(ETLConstants.Status.NEW))
            )
            
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.SUCCESS,
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered, True
            
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return None, False
    
    def _read_from_source(self) -> Optional[DataFrame]:
        """
        Read data from configured source.
        
        Returns:
            DataFrame or None if reading fails
        """
        source_config = self.config.get("data_sources", {}).get("raw_sales", {})
        source_format = source_config.get("format", "parquet")
        source_path = source_config.get("path")
        
        if not source_path:
            # Generate sample data if no source configured
            return self._generate_sample_data()
        
        try:
            if source_format == "parquet":
                return self.spark.read.schema(self.schema).parquet(source_path)
            elif source_format == "csv":
                return self.spark.read.schema(self.schema).csv(
                    source_path,
                    header=True
                )
            elif source_format == "jdbc":
                return self._read_from_jdbc(source_config)
            else:
                raise ValueError(f"Unsupported source format: {source_format}")
                
        except Exception as e:
            self.logger.log_message(
                step=ETLConstants.Step.EXTRACT,
                status=ETLConstants.Status.WARNING,
                message=f"Failed to read from source: {str(e)}. Using sample data."
            )
            return self._generate_sample_data()
    
    def _read_from_jdbc(self, source_config: dict) -> DataFrame:
        """
        Read data from JDBC source.
        
        Args:
            source_config: Source configuration
            
        Returns:
            DataFrame from JDBC source
        """
        db_config = self.config.get("database", {})
        table_name = self.config.get("tables", {}).get("raw_sales", "zsales_raw")
        
        return self.spark.read.format("jdbc").options(
            url=db_config.get("jdbc_url"),
            dbtable=table_name,
            driver=db_config.get("driver"),
            user=db_config.get("user"),
            password=db_config.get("password")
        ).load()
    
    def _generate_sample_data(self) -> DataFrame:
        """
        Generate sample data for demonstration.
        
        Returns:
            DataFrame with sample sales data
        """
        current_date = date.today()
        
        sample_data = [
            ("T000001", current_date, "CUST001", "PROD001", 10, 99.99, "USD", 
             "John Doe", "NORTH", "N", datetime.now(), "SYSTEM"),
            ("T000002", current_date, "CUST002", "PROD002", 5, 149.99, "USD", 
             "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
            ("T000003", current_date, "CUST003", "PROD001", 20, 99.99, "USD", 
             "John Doe", "EAST", "N", datetime.now(), "SYSTEM"),
            ("T000004", current_date, "CUST001", "PROD003", 3, 299.99, "USD", 
             "Bob Wilson", "WEST", "N", datetime.now(), "SYSTEM"),
            ("T000005", current_date, "CUST004", "PROD002", 15, 149.99, "USD", 
             "Jane Smith", "SOUTH", "N", datetime.now(), "SYSTEM"),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.schema)