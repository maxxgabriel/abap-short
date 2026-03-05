"""
ETL Loader Module
Handles loading of transformed analytics data into target systems
"""

from typing import Optional
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger
from src.config import ETLConfig
from src.exceptions import LoadError


class SalesDataLoader:
    """
    Loads transformed analytics data into target systems.
    
    Responsibilities:
    - Validate analytics data before loading
    - Write to target tables/files
    - Update source record status
    - Handle load errors
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the sales data loader.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: ETL configuration object
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load analytics data to target system.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            True if load successful, False otherwise
            
        Raises:
            LoadError: If load encounters critical error
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            # Validate data before loading
            valid_df = self._validate_records(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Write to target
            self._write_to_target(valid_df)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise LoadError(f"Load error: {str(e)}")
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate analytics records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame containing only valid records
        """
        # Validate required fields are not null
        valid_df = df.filter(
            F.col("analytics_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("product_id").isNotNull() &
            (F.col("gross_amount") > 0)
        )
        
        # Validate currency
        valid_df = valid_df.filter(F.col("currency").isNotNull())
        
        # Validate category
        valid_df = valid_df.filter(
            F.col("category").isin(["HIGH", "MEDIUM", "LOW"])
        )
        
        return valid_df
    
    def _write_to_target(self, df: DataFrame):
        """
        Write DataFrame to target system.
        
        Args:
            df: DataFrame to write
        """
        target_config = self.config.get_target_config()
        target_type = target_config.get("type", "parquet")
        target_path = target_config.get("path")
        
        if target_type == "jdbc":
            self._write_to_jdbc(df, target_config)
        elif target_type == "parquet":
            self._write_to_parquet(df, target_path)
        elif target_type == "delta":
            self._write_to_delta(df, target_path)
        else:
            # Default: write as parquet
            self._write_to_parquet(df, target_path)
    
    def _write_to_jdbc(self, df: DataFrame, jdbc_config: dict):
        """
        Write DataFrame to JDBC target.
        
        Args:
            df: DataFrame to write
            jdbc_config: JDBC connection configuration
        """
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", jdbc_config.get("table", "zsales_analytics")) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "com.sap.db.jdbc.Driver")) \
            .mode("append") \
            .save()
    
    def _write_to_parquet(self, df: DataFrame, path: str):
        """
        Write DataFrame to Parquet files.
        
        Args:
            df: DataFrame to write
            path: Target path for parquet files
        """
        df.write \
            .mode("append") \
            .partitionBy("trans_date") \
            .parquet(path)
    
    def _write_to_delta(self, df: DataFrame, path: str):
        """
        Write DataFrame to Delta Lake.
        
        Args:
            df: DataFrame to write
            path: Target path for delta table
        """
        df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("trans_date") \
            .save(path)