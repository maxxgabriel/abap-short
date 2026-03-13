"""
Data extraction module
Migrated from ABAP ZCL_ETL_EXTRACTOR
"""

from datetime import date
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

from src.config import get_config
from src.schemas import ETLSchemas
from src.logger import ETLLogger


class ETLExtractor:
    """
    Extract raw sales data from source
    Corresponds to ZCL_ETL_EXTRACTOR
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
        """
        self.spark = spark
        self.logger = logger
        self.config = get_config()
    
    def extract_data(
        self,
        from_date: date,
        to_date: date
    ) -> Tuple[bool, DataFrame]:
        """
        Extract raw sales data
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
        
        Returns:
            Tuple of (success flag, DataFrame)
        """
        try:
            self.logger.log_message(
                step=self.config.steps.EXTRACT,
                status=self.config.status.SUCCESS,
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get data source configuration
            source_config = self.config.get_data_source_config('raw_sales')
            
            # Read data based on format
            if source_config.get('format') == 'jdbc':
                sales_df = self._extract_from_jdbc(source_config, from_date, to_date)
            else:
                sales_df = self._extract_from_file(source_config, from_date, to_date)
            
            # Filter for new records only
            sales_df = sales_df.filter(
                (sales_df.trans_date >= lit(from_date)) &
                (sales_df.trans_date <= lit(to_date)) &
                (sales_df.status == self.config.status.NEW)
            )
            
            record_count = sales_df.count()
            
            self.logger.log_message(
                step=self.config.steps.EXTRACT,
                status=self.config.status.SUCCESS,
                message=f"Extracted {record_count} records successfully",
                records_processed=record_count,
                records_success=record_count
            )
            
            return True, sales_df
        
        except Exception as e:
            self.logger.log_message(
                step=self.config.steps.EXTRACT,
                status=self.config.status.ERROR,
                message=f"Extraction failed: {str(e)}"
            )
            return False, None
    
    def _extract_from_jdbc(
        self,
        source_config: dict,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """Extract data from JDBC source"""
        jdbc_config = source_config['jdbc']
        
        # Build query with date filter
        query = f"""
        (SELECT * FROM {jdbc_config['table']}
         WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
         AND status = '{self.config.status.NEW}') as sales_data
        """
        
        df = self.spark.read \
            .format('jdbc') \
            .option('url', jdbc_config['url']) \
            .option('dbtable', query) \
            .option('user', jdbc_config['user']) \
            .option('password', jdbc_config['password']) \
            .option('driver', jdbc_config['driver']) \
            .load()
        
        return df
    
    def _extract_from_file(
        self,
        source_config: dict,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """Extract data from file source"""
        df = self.spark.read \
            .format(source_config['format']) \
            .schema(ETLSchemas.raw_sales_schema()) \
            .load(source_config['path'])
        
        return df
    
    def create_sample_data(self, output_path: str = None) -> DataFrame:
        """
        Create sample raw sales data for testing
        Corresponds to sample data generation in ABAP code
        
        Args:
            output_path: Optional path to save sample data
        
        Returns:
            DataFrame with sample data
        """
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        df = self.spark.createDataFrame(
            sample_data,
            schema=ETLSchemas.raw_sales_schema()
        )
        
        # Add timestamp fields
        df = df.withColumn('created_at', current_timestamp()) \
               .withColumn('created_by', lit('SYSTEM'))
        
        if output_path:
            df.write.mode('overwrite').parquet(output_path)
        
        return df