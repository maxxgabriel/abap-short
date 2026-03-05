"""
Data Extraction Module
Extracts raw sales data from source with date range filtering
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional, Tuple
import logging

from src.exceptions import ETLExtractionError
from src.logger import ETLLogger


class RawSalesSchema:
    """Schema definition for raw sales data"""
    
    @staticmethod
    def get_schema() -> StructType:
        """Returns the StructType schema for raw sales data"""
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])


class SalesDataExtractor:
    """Extracts raw sales data from source system"""
    
    def __init__(
        self, 
        spark: SparkSession,
        logger: ETLLogger,
        config: dict
    ):
        """
        Initialize the extractor
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.source_config = config.get('source', {})
        
    def extract_data(
        self, 
        from_date: str,
        to_date: str
    ) -> Tuple[Optional[DataFrame], bool]:
        """
        Extract sales data for the given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Tuple of (DataFrame, success_flag)
            
        Raises:
            ETLExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Validate dates
            self._validate_date_range(from_date, to_date)
            
            # Build extraction query
            query = self._build_extraction_query(from_date, to_date)
            
            # Read data from source
            df = self._read_from_source(query)
            
            if df is None:
                raise ETLExtractionError("Failed to read data from source")
            
            # Apply schema and validate
            df = self._apply_schema_and_validate(df)
            
            # Cache for performance
            df.cache()
            
            record_count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            return df, True
            
        except Exception as e:
            error_msg = f'Extraction failed: {str(e)}'
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=error_msg
            )
            logging.error(error_msg, exc_info=True)
            raise ETLExtractionError(error_msg) from e
    
    def _validate_date_range(self, from_date: str, to_date: str) -> None:
        """Validate date range parameters"""
        try:
            from_dt = datetime.strptime(from_date, '%Y-%m-%d')
            to_dt = datetime.strptime(to_date, '%Y-%m-%d')
            
            if from_dt > to_dt:
                raise ValueError("from_date cannot be later than to_date")
                
            if to_dt > datetime.now():
                raise ValueError("to_date cannot be in the future")
                
        except ValueError as e:
            raise ETLExtractionError(f"Invalid date range: {str(e)}") from e
    
    def _build_extraction_query(self, from_date: str, to_date: str) -> str:
        """Build SQL query for data extraction"""
        table_name = self.source_config.get('table', 'ZSALES_RAW')
        
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
                status,
                created_at,
                created_by
            FROM {table_name}
            WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
              AND status = 'N'
            ORDER BY trans_date, trans_id
        """
        
        logging.info(f"Extraction query: {query}")
        return query
    
    def _read_from_source(self, query: str) -> Optional[DataFrame]:
        """
        Read data from source using JDBC
        
        Args:
            query: SQL query to execute
            
        Returns:
            DataFrame with extracted data
        """
        try:
            jdbc_options = {
                'url': self.source_config.get('url'),
                'driver': self.source_config.get('driver'),
                'query': query,
                'user': self.source_config.get('user'),
                'password': self.source_config.get('password'),
                'fetchsize': str(self.source_config.get('fetch_size', 1000))
            }
            
            # Add partitioning if configured
            if self.source_config.get('partition_column'):
                jdbc_options['partitionColumn'] = self.source_config['partition_column']
                jdbc_options['numPartitions'] = str(
                    self.source_config.get('num_partitions', 10)
                )
            
            df = self.spark.read \
                .format('jdbc') \
                .options(**jdbc_options) \
                .load()
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to read from source: {str(e)}")
            raise ETLExtractionError(f"Source read error: {str(e)}") from e
    
    def _apply_schema_and_validate(self, df: DataFrame) -> DataFrame:
        """
        Apply schema and validate data
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame with correct schema
        """
        try:
            schema = RawSalesSchema.get_schema()
            
            # Cast to expected types
            for field in schema.fields:
                if field.name in df.columns:
                    df = df.withColumn(field.name, df[field.name].cast(field.dataType))
            
            # Filter out null required fields
            required_fields = [
                f.name for f in schema.fields if not f.nullable
            ]
            
            for field in required_fields:
                df = df.filter(df[field].isNotNull())
            
            return df
            
        except Exception as e:
            raise ETLExtractionError(f"Schema validation error: {str(e)}") from e
    
    def extract_incremental(
        self,
        last_extracted_date: Optional[str] = None
    ) -> Tuple[Optional[DataFrame], bool]:
        """
        Extract data incrementally from last extracted date
        
        Args:
            last_extracted_date: Last successfully extracted date
            
        Returns:
            Tuple of (DataFrame, success_flag)
        """
        try:
            if last_extracted_date is None:
                # Default to last 7 days
                from datetime import timedelta
                from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            else:
                from_date = last_extracted_date
            
            to_date = datetime.now().strftime('%Y-%m-%d')
            
            return self.extract_data(from_date, to_date)
            
        except Exception as e:
            error_msg = f'Incremental extraction failed: {str(e)}'
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=error_msg
            )
            raise ETLExtractionError(error_msg) from e