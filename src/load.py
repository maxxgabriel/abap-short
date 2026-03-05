"""
Data Loading Module for Sales ETL System

Loads transformed data into target analytics table.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class SalesLoader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def load_data(
        self, 
        df_analytics: DataFrame,
        target_table: str = None
    ) -> Tuple[int, bool]:
        """
        Load analytics data into target table.
        
        Args:
            df_analytics: Analytics DataFrame to load
            target_table: Optional target table name override
            
        Returns:
            Tuple of (records_loaded, success_flag)
            
        Raises:
            ETLLoadError: If load fails
        """
        try:
            step = "LOAD"
            self.logger.log_message(
                step=step,
                status="S",
                message="Starting data load"
            )
            
            total_count = df_analytics.count()
            
            # Get target configuration
            table_name = target_table or self.config.get("target_table", "zsales_analytics")
            load_mode = self.config.get("load_mode", "append")
            
            # Write to target
            success_count = self._write_to_target(
                df_analytics, 
                table_name, 
                load_mode
            )
            
            # Update source status (if configured)
            if self.config.get("update_source_status", True):
                self._update_source_status(df_analytics)
            
            # Log load results
            self.logger.log_message(
                step=step,
                status="S",
                records_processed=total_count,
                records_success=success_count,
                records_error=total_count - success_count,
                message=f"Loaded {success_count} of {total_count} records"
            )
            
            return success_count, True
            
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=error_msg
            )
            raise ETLLoadError(error_msg, step="LOAD") from e
    
    def _write_to_target(
        self, 
        df: DataFrame, 
        table_name: str, 
        mode: str
    ) -> int:
        """
        Write DataFrame to target destination.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Number of records written
        """
        target_type = self.config.get("target_type", "jdbc")
        
        if target_type == "jdbc":
            return self._write_jdbc(df, table_name, mode)
        elif target_type == "parquet":
            return self._write_parquet(df, table_name, mode)
        elif target_type == "delta":
            return self._write_delta(df, table_name, mode)
        else:
            raise ValueError(f"Unsupported target type: {target_type}")
    
    def _write_jdbc(
        self, 
        df: DataFrame, 
        table_name: str, 
        mode: str
    ) -> int:
        """Write to JDBC target."""
        jdbc_config = self.config.get("jdbc", {})
        
        # Configure batch size for optimal performance
        batch_size = self.config.get("jdbc_batch_size", 1000)
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", table_name) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .option("batchsize", batch_size) \
            .mode(mode) \
            .save()
        
        return df.count()
    
    def _write_parquet(
        self, 
        df: DataFrame, 
        path: str, 
        mode: str
    ) -> int:
        """Write to Parquet files."""
        partition_cols = self.config.get("partition_columns", ["trans_date"])
        
        df.write \
            .partitionBy(*partition_cols) \
            .mode(mode) \
            .parquet(path)
        
        return df.count()
    
    def _write_delta(
        self, 
        df: DataFrame, 
        path: str, 
        mode: str
    ) -> int:
        """Write to Delta Lake."""
        partition_cols = self.config.get("partition_columns", ["trans_date"])
        
        df.write \
            .format("delta") \
            .partitionBy(*partition_cols) \
            .mode(mode) \
            .save(path)
        
        return df.count()
    
    def _update_source_status(self, df_analytics: DataFrame) -> None:
        """
        Update status of processed records in source table.
        
        Args:
            df_analytics: DataFrame with processed records
        """
        try:
            source_type = self.config.get("source_type", "jdbc")
            
            if source_type != "jdbc":
                self.log.info("Source status update only supported for JDBC sources")
                return
            
            # Extract transaction IDs from analytics data
            # Note: This assumes trans_id can be derived from analytics_id
            # In production, you'd maintain a mapping or include trans_id in analytics
            
            jdbc_config = self.config.get("jdbc", {})
            source_table = self.config.get("source_table", "zsales_raw")
            
            # For now, log the intent
            # In production, execute UPDATE statement via JDBC
            self.log.info(
                f"Would update {df_analytics.count()} records in {source_table} "
                "to status='P'"
            )
            
        except Exception as e:
            self.log.warning(f"Failed to update source status: {str(e)}")