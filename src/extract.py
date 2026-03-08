"""
PySpark Data Extraction Module
Extracts raw sales data from source systems
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from typing import Dict, Any, Optional
import logging
from datetime import datetime, date


class DataExtractor:
    """
    Handles extraction of raw sales data from various sources.
    Supports multiple data sources: files (CSV, Parquet), databases (JDBC), etc.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize data extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema
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
    
    def extract_from_csv(
        self,
        file_path: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            file_path: Path to CSV file
            from_date: Start date filter (optional)
            to_date: End date filter (optional)
            
        Returns:
            DataFrame with raw sales data
        """
        self.logger.info(f"Extracting data from CSV: {file_path}")
        
        df = (self.spark.read
              .option("header", "true")
              .option("inferSchema", "false")
              .schema(self.get_raw_sales_schema())
              .csv(file_path))
        
        # Apply date filters if provided
        if from_date:
            df = df.filter(df.trans_date >= from_date)
        if to_date:
            df = df.filter(df.trans_date <= to_date)
        
        # Filter only new records (status = 'N')
        df = df.filter(df.status == 'N')
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records from CSV")
        
        return df
    
    def extract_from_parquet(
        self,
        file_path: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> DataFrame:
        """
        Extract data from Parquet file.
        
        Args:
            file_path: Path to Parquet file
            from_date: Start date filter (optional)
            to_date: End date filter (optional)
            
        Returns:
            DataFrame with raw sales data
        """
        self.logger.info(f"Extracting data from Parquet: {file_path}")
        
        df = self.spark.read.parquet(file_path)
        
        # Apply date filters
        if from_date:
            df = df.filter(df.trans_date >= from_date)
        if to_date:
            df = df.filter(df.trans_date <= to_date)
        
        # Filter only new records
        df = df.filter(df.status == 'N')
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records from Parquet")
        
        return df
    
    def extract_from_jdbc(
        self,
        table_name: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> DataFrame:
        """
        Extract data from database via JDBC.
        
        Args:
            table_name: Name of source table
            from_date: Start date filter (optional)
            to_date: End date filter (optional)
            
        Returns:
            DataFrame with raw sales data
        """
        self.logger.info(f"Extracting data from JDBC table: {table_name}")
        
        jdbc_config = self.config['source']['jdbc']
        
        # Build query with filters
        query = f"SELECT * FROM {table_name} WHERE status = 'N'"
        if from_date:
            query += f" AND trans_date >= '{from_date}'"
        if to_date:
            query += f" AND trans_date <= '{to_date}'"
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config['url'])
              .option("dbtable", f"({query}) AS sales_data")
              .option("user", jdbc_config['user'])
              .option("password", jdbc_config['password'])
              .option("driver", jdbc_config['driver'])
              .load())
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records from JDBC")
        
        return df
    
    def extract(
        self,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> DataFrame:
        """
        Extract data based on configured source type.
        
        Args:
            from_date: Start date filter (optional)
            to_date: End date filter (optional)
            
        Returns:
            DataFrame with raw sales data
        """
        source_type = self.config['source']['type']
        source_path = self.config['source']['path']
        
        self.logger.info(f"Starting extraction from {source_type}")
        start_time = datetime.now()
        
        if source_type == 'csv':
            df = self.extract_from_csv(source_path, from_date, to_date)
        elif source_type == 'parquet':
            df = self.extract_from_parquet(source_path, from_date, to_date)
        elif source_type == 'jdbc':
            table_name = self.config['source']['table']
            df = self.extract_from_jdbc(table_name, from_date, to_date)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Extraction completed in {duration:.2f} seconds")
        
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data quality.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info("Validating extracted data")
        
        # Check if DataFrame is empty
        if df.count() == 0:
            self.logger.warning("No records extracted")
            return True  # Not an error, just no data
        
        # Check for null values in required fields
        required_fields = ['trans_id', 'trans_date', 'customer_id', 'product_id', 
                          'quantity', 'unit_price', 'currency']
        
        for field in required_fields:
            null_count = df.filter(df[field].isNull()).count()
            if null_count > 0:
                self.logger.error(f"Found {null_count} null values in required field: {field}")
                return False
        
        # Check for negative quantities or prices
        invalid_quantity = df.filter(df.quantity <= 0).count()
        if invalid_quantity > 0:
            self.logger.error(f"Found {invalid_quantity} records with invalid quantity")
            return False
        
        invalid_price = df.filter(df.unit_price <= 0).count()
        if invalid_price > 0:
            self.logger.error(f"Found {invalid_price} records with invalid unit_price")
            return False
        
        self.logger.info("Data validation passed")
        return True


if __name__ == "__main__":
    # Test extraction module
    import yaml
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create Spark session
    spark = SparkSession.builder.appName("ExtractorTest").getOrCreate()
    
    # Create extractor
    extractor = DataExtractor(spark, config)
    
    # Test extraction
    df = extractor.extract()
    print(f"Extracted {df.count()} records")
    df.show(5)
    
    # Validate
    is_valid = extractor.validate_extracted_data(df)
    print(f"Validation result: {is_valid}")
    
    spark.stop()