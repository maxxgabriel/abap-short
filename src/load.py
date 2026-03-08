"""
Load module for Sales ETL process.
Loads transformed analytics data into target table.
"""

import logging
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.logger import ETLLogger


logger = logging.getLogger(__name__)


class Loader:
    """Handles data loading to target analytics table."""
    
    def __init__(self, spark: SparkSession, etl_logger: ETLLogger, config: dict):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            etl_logger: ETL logging instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_logger = etl_logger
        self.config = config
        
    def load_data(
        self, 
        df_analytics: DataFrame,
        target_table: str = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target table.
        
        Args:
            df_analytics: Analytics DataFrame to load
            target_table: Optional override for target table name
            mode: Write mode (append, overwrite)
            
        Returns:
            Success flag
        """
        step = "LOAD"
        
        try:
            self.etl_logger.log_message(
                step=step,
                status="S",
                message="Starting data load"
            )
            
            record_count = df_analytics.count()
            
            # Get target table name
            table_name = target_table or self.config.get("target_table", "zsales_analytics")
            
            # Validate records before load
            df_valid = self._validate_records(df_analytics)
            valid_count = df_valid.count()
            error_count = record_count - valid_count
            
            if error_count > 0:
                self.etl_logger.log_message(
                    step=step,
                    status="W",
                    records_error=error_count,
                    message=f"Skipped {error_count} invalid records"
                )
            
            # Write to target table
            df_valid.write.mode(mode).saveAsTable(table_name)
            
            # Update source table status
            self._update_source_status(df_valid)
            
            self.etl_logger.log_message(
                step=step,
                status="S",
                records_processed=record_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {record_count} records to {table_name}"
            )
            
            logger.info(f"Loaded {valid_count} records to {table_name}")
            
            return True
            
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.etl_logger.log_message(
                step=step,
                status="E",
                message=error_msg
            )
            logger.error(error_msg, exc_info=True)
            return False
            
    def load_to_path(
        self,
        df_analytics: DataFrame,
        output_path: str,
        format: str = "parquet",
        partition_by: list = None
    ) -> bool:
        """
        Load analytics data to file path.
        
        Args:
            df_analytics: Analytics DataFrame to load
            output_path: Output path for data
            format: Output format (parquet, csv, json)
            partition_by: Optional list of columns to partition by
            
        Returns:
            Success flag
        """
        step = "LOAD"
        
        try:
            self.etl_logger.log_message(
                step=step,
                status="S",
                message=f"Starting data load to {output_path}"
            )
            
            record_count = df_analytics.count()
            
            # Validate records
            df_valid = self._validate_records(df_analytics)
            valid_count = df_valid.count()
            
            # Write to path
            writer = df_valid.write.mode("overwrite")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
                
            if format.lower() == "csv":
                writer.csv(output_path, header=True)
            elif format.lower() == "json":
                writer.json(output_path)
            else:  # Default to parquet
                writer.parquet(output_path)
            
            self.etl_logger.log_message(
                step=step,
                status="S",
                records_processed=record_count,
                records_success=valid_count,
                message=f"Loaded {valid_count} records to {output_path}"
            )
            
            logger.info(f"Loaded {valid_count} records to {output_path}")
            
            return True
            
        except Exception as e:
            error_msg = f"Load to path failed: {str(e)}"
            self.etl_logger.log_message(
                step=step,
                status="E",
                message=error_msg
            )
            logger.error(error_msg, exc_info=True)
            return False
            
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        # Check required fields
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return df_valid
        
    def _update_source_status(self, df: DataFrame):
        """
        Update status in source table for processed records.
        
        Args:
            df: DataFrame with processed analytics records
        """
        try:
            source_table = self.config.get("source_table", "zsales_raw")
            
            # In production, this would execute an UPDATE statement
            # For now, we log the intent
            record_count = df.count()
            
            self.etl_logger.log_message(
                step="LOAD",
                status="I",
                records_processed=record_count,
                message=f"Source status update required for {record_count} records in {source_table}"
            )
            
            # Example UPDATE (would be executed via JDBC or Delta Lake merge):
            # UPDATE zsales_raw SET status = 'P' 
            # WHERE trans_id IN (SELECT trans_id FROM processed_records)
            
            logger.info(f"Status update logged for {record_count} source records")
            
        except Exception as e:
            logger.warning(f"Source status update failed: {str(e)}")