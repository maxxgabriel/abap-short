"""Data extraction module."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Optional
from datetime import datetime

from src.logger import ETLLogger
from src.schemas import ETLSchemas
from src.resilience import with_retry


class DataExtractor:
    """Extracts raw sales data from source."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """Initialize data extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Extractor configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.source_config = config.get('sources', {}).get('raw_sales', {})
    
    @with_retry
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        checkpoint_path: Optional[str] = None
    ) -> DataFrame:
        """Extract raw sales data for date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            checkpoint_path: Optional checkpoint path to save data
            
        Returns:
            DataFrame containing raw sales data
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='I',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Read raw data
            source_path = self.source_config.get('path')
            source_format = self.source_config.get('format', 'parquet')
            
            df = self.spark.read \
                .format(source_format) \
                .schema(ETLSchemas.raw_sales_schema()) \
                .load(source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (col('trans_date') >= lit(from_date)) &
                (col('trans_date') <= lit(to_date)) &
                (col('status') == lit('N'))
            )
            
            # Cache for performance
            df_filtered.cache()
            
            record_count = df_filtered.count()
            
            # Save checkpoint if requested
            if checkpoint_path:
                df_filtered.write.mode('overwrite').parquet(checkpoint_path)
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Extracted {record_count} records successfully',
                records_processed=record_count,
                records_success=record_count,
                records_error=0
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}',
                error_details=str(e)
            )
            raise
    
    def create_sample_data(self) -> DataFrame:
        """Create sample data for testing.
        
        Returns:
            DataFrame containing sample sales data
        """
        from pyspark.sql.types import Row
        from datetime import date
        
        sample_data = [
            Row(
                trans_id='T000001',
                trans_date=date.today(),
                customer_id='CUST001',
                product_id='PROD001',
                quantity=10,
                unit_price=99.99,
                currency='USD',
                sales_rep='John Doe',
                region='NORTH',
                status='N'
            ),
            Row(
                trans_id='T000002',
                trans_date=date.today(),
                customer_id='CUST002',
                product_id='PROD002',
                quantity=5,
                unit_price=149.99,
                currency='USD',
                sales_rep='Jane Smith',
                region='SOUTH',
                status='N'
            ),
            Row(
                trans_id='T000003',
                trans_date=date.today(),
                customer_id='CUST003',
                product_id='PROD001',
                quantity=20,
                unit_price=99.99,
                currency='USD',
                sales_rep='John Doe',
                region='EAST',
                status='N'
            ),
            Row(
                trans_id='T000004',
                trans_date=date.today(),
                customer_id='CUST001',
                product_id='PROD003',
                quantity=3,
                unit_price=299.99,
                currency='USD',
                sales_rep='Bob Wilson',
                region='WEST',
                status='N'
            ),
            Row(
                trans_id='T000005',
                trans_date=date.today(),
                customer_id='CUST004',
                product_id='PROD002',
                quantity=15,
                unit_price=149.99,
                currency='USD',
                sales_rep='Jane Smith',
                region='SOUTH',
                status='N'
            ),
        ]
        
        return self.spark.createDataFrame(sample_data, ETLSchemas.raw_sales_schema())