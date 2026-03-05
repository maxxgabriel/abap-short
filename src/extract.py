"""
ETL Extractor Module
Extracts raw sales data from source systems.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Optional
import yaml
import logging


class ETLExtractor:
    """Extracts raw sales data from source tables."""
    
    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        """
        Initialize extractor with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config_path: Path to configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema for raw sales DataFrame
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
        ])
    
    def extract_data(
        self,
        from_date: str,
        to_date: str,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date (format: YYYY-MM-DD)
            to_date: End date (format: YYYY-MM-DD)
            source_path: Optional path to source data file/table
            
        Returns:
            DataFrame with extracted raw sales data
        """
        self.logger.info(f"Starting extraction from {from_date} to {to_date}")
        
        try:
            # Use provided source path or get from config
            path = source_path or self.config['data_sources']['raw_sales_path']
            
            # Read data based on source type
            if path.endswith('.csv'):
                df = self._extract_from_csv(path)
            elif path.endswith('.parquet'):
                df = self._extract_from_parquet(path)
            elif path.startswith('jdbc:'):
                df = self._extract_from_database(path)
            else:
                raise ValueError(f"Unsupported source type: {path}")
            
            # Filter by date range and status
            df_filtered = df.filter(
                (F.col("trans_date") >= F.lit(from_date)) &
                (F.col("trans_date") <= F.lit(to_date)) &
                (F.col("status") == F.lit("N"))
            )
            
            record_count = df_filtered.count()
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_csv(self, path: str) -> DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            path: Path to CSV file
            
        Returns:
            DataFrame with raw data
        """
        self.logger.info(f"Reading CSV from {path}")
        
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(self.get_raw_sales_schema()) \
            .csv(path)
    
    def _extract_from_parquet(self, path: str) -> DataFrame:
        """
        Extract data from Parquet file.
        
        Args:
            path: Path to Parquet file
            
        Returns:
            DataFrame with raw data
        """
        self.logger.info(f"Reading Parquet from {path}")
        
        return self.spark.read \
            .schema(self.get_raw_sales_schema()) \
            .parquet(path)
    
    def _extract_from_database(self, jdbc_url: str) -> DataFrame:
        """
        Extract data from database via JDBC.
        
        Args:
            jdbc_url: JDBC connection URL
            
        Returns:
            DataFrame with raw data
        """
        self.logger.info(f"Reading from database: {jdbc_url}")
        
        db_config = self.config['data_sources']['database']
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_config['table']) \
            .option("user", db_config.get('user', '')) \
            .option("password", db_config.get('password', '')) \
            .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
            .load()
    
    def validate_extracted_data(self, df: DataFrame) -> dict:
        """
        Validate extracted data quality.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating extracted data")
        
        total_records = df.count()
        
        # Check for null critical fields
        null_counts = {
            "trans_id": df.filter(F.col("trans_id").isNull()).count(),
            "trans_date": df.filter(F.col("trans_date").isNull()).count(),
            "customer_id": df.filter(F.col("customer_id").isNull()).count(),
            "product_id": df.filter(F.col("product_id").isNull()).count(),
            "quantity": df.filter(F.col("quantity").isNull()).count(),
            "unit_price": df.filter(F.col("unit_price").isNull()).count(),
        }
        
        # Check for invalid values
        invalid_quantity = df.filter(F.col("quantity") <= 0).count()
        invalid_price = df.filter(F.col("unit_price") <= 0).count()
        
        validation_results = {
            "total_records": total_records,
            "null_counts": null_counts,
            "invalid_quantity": invalid_quantity,
            "invalid_price": invalid_price,
            "is_valid": (
                sum(null_counts.values()) == 0 and
                invalid_quantity == 0 and
                invalid_price == 0
            )
        }
        
        self.logger.info(f"Validation results: {validation_results}")
        
        return validation_results