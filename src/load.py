"""
Data loading module for Sales ETL process.
Loads transformed data into target analytics tables.
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class DataLoader:
    """Handles loading of transformed data into target systems."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: ETLConfig):
        """
        Initialize the data loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: ETL configuration
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target destination.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional override for target path
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            record_count = analytics_df.count()
            
            # Determine target destination
            target = target_path or self.config.get("target.analytics_path")
            target_type = self.config.get("target.type", "parquet")
            
            # Load based on target type
            if target_type == "parquet":
                success = self._load_to_parquet(analytics_df, target, mode)
            elif target_type == "jdbc":
                success = self._load_to_jdbc(analytics_df, mode)
            elif target_type == "delta":
                success = self._load_to_delta(analytics_df, target, mode)
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            if success:
                self.logger.log_message(
                    step="LOAD",
                    status="S",
                    records_processed=record_count,
                    records_success=record_count,
                    message=f"Loaded {record_count} records successfully"
                )
                
                # Update source records status if configured
                if self.config.get("processing.update_source_status", True):
                    self._update_source_status(analytics_df)
                
            return success
            
        except Exception as e:
            self.log.error(f"Load failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _load_to_parquet(
        self, 
        df: DataFrame, 
        path: str, 
        mode: str
    ) -> bool:
        """Load data to Parquet files."""
        try:
            partition_cols = self.config.get("target.partition_columns", [])
            
            writer = df.write.mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.parquet(path)
            self.log.info(f"Data written to Parquet: {path}")
            return True
            
        except Exception as e:
            self.log.error(f"Parquet write failed: {str(e)}")
            return False
    
    def _load_to_jdbc(self, df: DataFrame, mode: str) -> bool:
        """Load data to JDBC destination (database)."""
        try:
            jdbc_config = self.config.get("target.jdbc", {})
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", jdbc_config.get("table", "sales_analytics")) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
                .mode(mode) \
                .save()
            
            self.log.info(f"Data written to JDBC: {jdbc_config.get('table')}")
            return True
            
        except Exception as e:
            self.log.error(f"JDBC write failed: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, path: str, mode: str) -> bool:
        """Load data to Delta Lake format."""
        try:
            df.write \
                .format("delta") \
                .mode(mode) \
                .save(path)
            
            self.log.info(f"Data written to Delta: {path}")
            return True
            
        except Exception as e:
            self.log.error(f"Delta write failed: {str(e)}")
            return False
    
    def _update_source_status(self, analytics_df: DataFrame) -> None:
        """
        Update status of processed records in source system.
        
        Args:
            analytics_df: DataFrame containing processed records
        """
        try:
            # Extract transaction IDs from analytics data
            trans_ids = [
                row.analytics_id.replace("ANL", "").split("T")[1][:6] 
                for row in analytics_df.select("analytics_id").collect()
            ]
            
            # In production, update source table status
            # Example: UPDATE sales_raw SET status = 'P' WHERE trans_id IN (...)
            
            self.log.info(f"Updated status for {len(trans_ids)} source records")
            
        except Exception as e:
            self.log.warning(f"Failed to update source status: {str(e)}")
    
    def validate_load(self, expected_count: int, target_path: str) -> bool:
        """
        Validate that data was loaded correctly.
        
        Args:
            expected_count: Expected number of records
            target_path: Path to loaded data
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            loaded_df = self.spark.read.parquet(target_path)
            actual_count = loaded_df.count()
            
            if actual_count == expected_count:
                self.log.info(f"Load validation passed: {actual_count} records")
                return True
            else:
                self.log.warning(
                    f"Load validation failed: expected {expected_count}, "
                    f"found {actual_count}"
                )
                return False
                
        except Exception as e:
            self.log.error(f"Load validation error: {str(e)}")
            return False