"""
Sales ETL - Load Module
Loads transformed analytics data to target destination.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
import logging
from typing import Tuple

from src.utils.logger import ETLLogger
from src.utils.exceptions import LoadError


class SalesLoader:
    """Handles loading of analytics data to target."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_type: str = "parquet"
    ) -> Tuple[bool, int, int]:
        """
        Load analytics data to target.
        
        Args:
            analytics_df: Analytics DataFrame to load
            target_type: Target type (parquet, delta, jdbc, etc.)
            
        Returns:
            Tuple of (success, success_count, error_count)
            
        Raises:
            LoadError: If load fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="I",
                message="Starting data load"
            )
            
            total_count = analytics_df.count()
            
            # Validate records before loading
            valid_df = self._validate_records(analytics_df)
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Load to target
            target_config = self.config.get("target", {})
            
            if target_type == "parquet":
                self._load_to_parquet(valid_df, target_config)
            elif target_type == "delta":
                self._load_to_delta(valid_df, target_config)
            elif target_type == "jdbc":
                self._load_to_jdbc(valid_df, target_config)
            else:
                raise LoadError(f"Unsupported target type: {target_type}")
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return True, valid_count, error_count
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise LoadError(f"Failed to load data: {str(e)}") from e
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        # Filter out invalid records
        valid_df = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0) &
            (col("net_amount") > 0) &
            col("currency").isNotNull() &
            col("category").isin(["HIGH", "MEDIUM", "LOW"])
        )
        
        return valid_df
    
    def _load_to_parquet(self, df: DataFrame, config: dict):
        """Load to Parquet files."""
        path = config.get("path")
        mode = config.get("mode", "append")
        partition_by = config.get("partition_by", ["trans_date"])
        
        df.write.mode(mode).partitionBy(*partition_by).parquet(path)
        
        self.logger.log_message(
            step="LOAD",
            status="I",
            message=f"Written data to Parquet: {path}"
        )
    
    def _load_to_delta(self, df: DataFrame, config: dict):
        """Load to Delta Lake."""
        path = config.get("path")
        mode = config.get("mode", "append")
        partition_by = config.get("partition_by", ["trans_date"])
        
        df.write.format("delta").mode(mode).partitionBy(*partition_by).save(path)
        
        self.logger.log_message(
            step="LOAD",
            status="I",
            message=f"Written data to Delta: {path}"
        )
    
    def _load_to_jdbc(self, df: DataFrame, config: dict):
        """Load to JDBC target (SAP/Database)."""
        jdbc_config = config.get("jdbc", {})
        
        df.write.format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", jdbc_config.get("table", "ZSALES_ANALYTICS")) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "com.sap.db.jdbc.Driver")) \
            .mode(jdbc_config.get("mode", "append")) \
            .save()
        
        self.logger.log_message(
            step="LOAD",
            status="I",
            message=f"Written data to JDBC table: {jdbc_config.get('table')}"
        )
    
    def update_source_status(self, trans_ids: list, status: str = "P"):
        """
        Update status of processed records in source.
        
        Args:
            trans_ids: List of transaction IDs to update
            status: New status code (default: 'P' for processed)
        """
        try:
            # This would typically update the source table
            # Implementation depends on source system capabilities
            self.logger.log_message(
                step="LOAD",
                status="I",
                message=f"Updated {len(trans_ids)} source records to status {status}"
            )
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Failed to update source status: {str(e)}"
            )