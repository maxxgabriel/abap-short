"""
Load module for Sales ETL System.
Loads transformed analytics data into target destination.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Optional
import logging


class SalesLoader:
    """Loads transformed analytics data into target."""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            logger: Logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def load_data(self, df_analytics: DataFrame, target_path: str, mode: str = "append") -> int:
        """
        Load analytics data to target destination.
        
        Args:
            df_analytics: Analytics DataFrame to load
            target_path: Target path for data
            mode: Write mode (append, overwrite, etc.)
        
        Returns:
            Number of records loaded
        
        Raises:
            Exception: If load fails
        """
        try:
            self.logger.info(f"Starting data load to {target_path}")
            
            # Validate records before loading
            df_valid = self._validate_records(df_analytics)
            
            record_count = df_valid.count()
            
            # Write to target (Parquet format)
            df_valid.write.mode(mode).parquet(target_path)
            
            self.logger.info(f"Loaded {record_count} records successfully")
            
            return record_count
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            DataFrame with only valid records
        """
        # Filter out invalid records
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        invalid_count = df.count() - df_valid.count()
        if invalid_count > 0:
            self.logger.warning(f"Filtered out {invalid_count} invalid records")
        
        return df_valid
    
    def load_to_database(self, df_analytics: DataFrame, jdbc_url: str, 
                        table_name: str, properties: dict, mode: str = "append") -> int:
        """
        Load analytics data to database via JDBC.
        
        Args:
            df_analytics: Analytics DataFrame to load
            jdbc_url: JDBC connection URL
            table_name: Target table name
            properties: JDBC connection properties
            mode: Write mode (append, overwrite, etc.)
        
        Returns:
            Number of records loaded
        
        Raises:
            Exception: If load fails
        """
        try:
            self.logger.info(f"Starting data load to database table {table_name}")
            
            # Validate records before loading
            df_valid = self._validate_records(df_analytics)
            
            record_count = df_valid.count()
            
            # Write to database
            df_valid.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties=properties
            )
            
            self.logger.info(f"Loaded {record_count} records to database successfully")
            
            return record_count
            
        except Exception as e:
            self.logger.error(f"Database load failed: {str(e)}")
            raise


class LoaderInterface:
    """Interface contract for all loader components."""
    
    def load_data(self, df: DataFrame, target: str, **kwargs) -> int:
        """Load data to target destination."""
        raise NotImplementedError("Subclasses must implement load_data()")
    
    def get_component_name(self) -> str:
        """Return component name."""
        raise NotImplementedError("Subclasses must implement get_component_name()")
    
    def validate_target(self, target: str) -> bool:
        """Validate target destination is accessible."""
        raise NotImplementedError("Subclasses must implement validate_target()")