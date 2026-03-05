"""
Data Load Module
Loads transformed analytics data into target storage.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class DataLoader:
    """
    Loads transformed data into target analytics table.
    Handles validation, data quality checks, and status updates.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the data loader.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance for logging
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(
        self, 
        df_analytics: DataFrame,
        target_path: str = None
    ) -> Tuple[bool, int, int]:
        """
        Load transformed data into target storage.
        
        Args:
            df_analytics: Transformed analytics DataFrame
            target_path: Optional override for target path
            
        Returns:
            Tuple of (success boolean, success count, error count)
            
        Raises:
            ETLLoadError: If load operation fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="I",
                message="Starting data load"
            )
            
            total_count = df_analytics.count()
            
            # Validate all records before loading
            df_valid, df_invalid = self._validate_records(df_analytics)
            
            valid_count = df_valid.count()
            invalid_count = df_invalid.count()
            
            # Log invalid records if any
            if invalid_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Found {invalid_count} invalid records, will skip"
                )
                self._log_invalid_records(df_invalid)
            
            # Load valid records
            if valid_count > 0:
                self._write_to_target(df_valid, target_path)
                
                # Update source status (mark as processed)
                self._update_source_status(df_valid)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=invalid_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return True, valid_count, invalid_count
            
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=error_msg
            )
            raise ETLLoadError(error_msg, step="LOAD") from e
    
    def _validate_records(
        self, 
        df: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Validate records and separate valid from invalid.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (valid DataFrame, invalid DataFrame)
        """
        # Define validation conditions
        validation_condition = (
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        # Split into valid and invalid
        df_valid = df.filter(validation_condition)
        df_invalid = df.filter(~validation_condition)
        
        return df_valid, df_invalid
    
    def _write_to_target(self, df: DataFrame, target_path: str = None) -> None:
        """
        Write data to target storage.
        
        Args:
            df: DataFrame to write
            target_path: Optional override for target path
        """
        target = target_path or self.config.get("target_path")
        target_format = self.config.get("target_format", "parquet")
        write_mode = self.config.get("write_mode", "append")
        
        if target_format == "jdbc":
            self._write_to_jdbc(df, target)
        elif target_format == "delta":
            self._write_to_delta(df, target, write_mode)
        elif target_format == "parquet":
            self._write_to_parquet(df, target, write_mode)
        elif target_format == "csv":
            self._write_to_csv(df, target, write_mode)
        else:
            raise ValueError(f"Unsupported target format: {target_format}")
    
    def _write_to_jdbc(self, df: DataFrame, connection_config: dict) -> None:
        """Write data to JDBC target (e.g., SAP HANA, Oracle)."""
        jdbc_url = connection_config.get("url")
        table_name = connection_config.get("table", "zsales_analytics")
        write_mode = connection_config.get("mode", "append")
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", connection_config.get("user")) \
            .option("password", connection_config.get("password")) \
            .option("driver", connection_config.get("driver", "com.sap.db.jdbc.Driver")) \
            .mode(write_mode) \
            .save()
    
    def _write_to_delta(self, df: DataFrame, path: str, mode: str) -> None:
        """Write data to Delta Lake format."""
        df.write \
            .format("delta") \
            .mode(mode) \
            .save(path)
    
    def _write_to_parquet(self, df: DataFrame, path: str, mode: str) -> None:
        """Write data to Parquet format."""
        df.write \
            .format("parquet") \
            .mode(mode) \
            .partitionBy("trans_date") \
            .save(path)
    
    def _write_to_csv(self, df: DataFrame, path: str, mode: str) -> None:
        """Write data to CSV format."""
        df.write \
            .format("csv") \
            .option("header", "true") \
            .mode(mode) \
            .save(path)
    
    def _update_source_status(self, df_loaded: DataFrame) -> None:
        """
        Update source records status to 'P' (processed).
        In production, this would update the source table.
        
        Args:
            df_loaded: DataFrame that was successfully loaded
        """
        # Get transaction IDs that were loaded
        trans_ids = df_loaded.select("analytics_id").distinct()
        
        # Log status update
        count = trans_ids.count()
        self.logger.log_message(
            step="LOAD",
            status="I",
            message=f"Marking {count} source records as processed"
        )
        
        # In production: UPDATE zsales_raw SET status = 'P' WHERE trans_id IN (...)
        # For now, just log the action
    
    def _log_invalid_records(self, df_invalid: DataFrame) -> None:
        """
        Log details about invalid records for debugging.
        
        Args:
            df_invalid: DataFrame containing invalid records
        """
        # Sample a few invalid records for logging
        invalid_sample = df_invalid.limit(10).collect()
        
        for idx, row in enumerate(invalid_sample):
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Invalid record {idx+1}: analytics_id={row.analytics_id}"
            )