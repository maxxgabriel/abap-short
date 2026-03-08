"""
Data Extraction Module
Replaces ZCL_ETL_EXTRACTOR class
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType
)

from src.utils.logger import ETLLogger
from src.utils.exceptions import ETLError


class DataExtractor:
    """
    Extracts raw sales data from source
    Replaces ZCL_ETL_EXTRACTOR
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        logger: ETLLogger
    ):
        """Initialize extractor"""
        self.spark = spark
        self.config = config
        self.logger = logger
    
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
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source configuration
            source_config = self.config['database']['source']
            
            # Read data based on format
            if source_config['format'] == 'jdbc':
                df = self._extract_from_jdbc(source_config, from_date, to_date)
            elif source_config['format'] == 'parquet':
                df = self._extract_from_parquet(source_config, from_date, to_date)
            elif source_config['format'] == 'delta':
                df = self._extract_from_delta(source_config, from_date, to_date)
            else:
                raise ETLError(f"Unsupported source format: {source_config['format']}")
            
            # Filter for new records only (status = 'N')
            status_new = self.config['etl']['status_codes']['new']
            df = df.filter(df.status == status_new)
            
            record_count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f"Extraction failed: {str(e)}"
            )
            raise ETLError(f"Extraction failed: {str(e)}") from e
    
    def _extract_from_jdbc(
        self,
        config: dict,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """Extract data from JDBC source"""
        query = f"""
        (SELECT 
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
        FROM {config['table']}
        WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
        ) AS source_data
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", config['url']) \
            .option("dbtable", query) \
            .option("driver", config['driver']) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .option("fetchsize", config.get('fetch_size', 1000)) \
            .load()
        
        return df
    
    def _extract_from_parquet(
        self,
        config: dict,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """Extract data from Parquet files"""
        df = self.spark.read.parquet(config['path'])
        
        # Filter by date range
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date)
        )
        
        return df
    
    def _extract_from_delta(
        self,
        config: dict,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """Extract data from Delta Lake"""
        df = self.spark.read.format("delta").load(config['path'])
        
        # Filter by date range
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date)
        )
        
        return df
    
    @staticmethod
    def get_schema() -> StructType:
        """Get schema for raw sales data"""
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