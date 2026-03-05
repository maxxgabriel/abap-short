"""
ETL Logger with Delta Lake Audit Trail
Converts ABAP logger methods to PySpark DataFrame operations
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from datetime import datetime
from typing import Optional
import uuid


class ETLLogger:
    """
    PySpark logger implementation that writes audit logs to Delta Lake
    Maps to ZETL_LOG table schema from ABAP
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, delta_path: str):
        """
        Initialize ETL Logger
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique ETL run identifier
            delta_path: Path to Delta Lake table for logs
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.delta_path = delta_path
        self._log_buffer = []
        
    @staticmethod
    def get_log_schema() -> StructType:
        """
        Returns the schema for ZETL_LOG table
        Maps to ABAP ZETL_LOG structure
        """
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("created_by", StringType(), True)
        ])
    
    def log_message(
        self,
        process_step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message to the audit trail
        Maps to ABAP ZCL_ETL_LOGGER->log_message method
        
        Args:
            process_step: ETL step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now.strftime("%H:%M:%S"),
            "execution_timestamp": now,
            "process_step": process_step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "created_at": now,
            "created_by": "pyspark_etl"
        }
        
        self._log_buffer.append(log_entry)
        
        # Also print to console for immediate feedback
        print(f"[{now.strftime('%H:%M:%S')}] {process_step} | {status} | {message}")
        
        if records_processed > 0:
            print(f"  → Processed: {records_processed}, Success: {records_success}, Errors: {records_error}")
    
    def flush_logs(self) -> None:
        """
        Write buffered logs to Delta Lake
        """
        if not self._log_buffer:
            return
        
        try:
            # Create DataFrame from buffer
            log_df = self.spark.createDataFrame(self._log_buffer, schema=self.get_log_schema())
            
            # Write to Delta Lake in append mode
            log_df.write \
                .format("delta") \
                .mode("append") \
                .save(self.delta_path)
            
            print(f"✓ Flushed {len(self._log_buffer)} log entries to Delta Lake")
            
            # Clear buffer after successful write
            self._log_buffer.clear()
            
        except Exception as e:
            print(f"✗ Error flushing logs to Delta Lake: {str(e)}")
            raise
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID
        Maps to ABAP ZCL_ETL_LOGGER->get_etl_run_id method
        """
        return self.etl_run_id
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID
        Maps to ABAP generate_log_id method
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_suffix = str(uuid.uuid4())[:8]
        return f"LOG{timestamp}{unique_suffix}"
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """
        Generate unique ETL run ID
        Maps to ABAP generate_etl_run_id method
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_suffix = str(uuid.uuid4())[:8]
        return f"ETL{timestamp}{unique_suffix}"
    
    def read_logs(self, filters: Optional[dict] = None) -> DataFrame:
        """
        Read logs from Delta Lake with optional filters
        
        Args:
            filters: Dictionary of column->value filters
            
        Returns:
            DataFrame containing filtered logs
        """
        try:
            df = self.spark.read.format("delta").load(self.delta_path)
            
            if filters:
                for column, value in filters.items():
                    df = df.filter(df[column] == value)
            
            return df
            
        except Exception as e:
            print(f"✗ Error reading logs from Delta Lake: {str(e)}")
            raise
    
    def get_run_summary(self) -> DataFrame:
        """
        Get summary statistics for the current ETL run
        
        Returns:
            DataFrame with aggregated statistics
        """
        try:
            df = self.read_logs(filters={"etl_run_id": self.etl_run_id})
            
            summary = df.groupBy("process_step", "status").agg({
                "records_processed": "sum",
                "records_success": "sum",
                "records_error": "sum"
            })
            
            return summary
            
        except Exception as e:
            print(f"✗ Error generating run summary: {str(e)}")
            raise


class LogConstants:
    """
    Constants for logging
    Maps to ABAP ZCL_ETL_CONSTANTS status and step codes
    """
    
    # Status codes
    class Status:
        NEW = "N"
        PROCESSED = "P"
        ERROR = "E"
        WARNING = "W"
        SUCCESS = "S"
        INFO = "I"
    
    # Process steps
    class Step:
        INIT = "INIT"
        EXTRACT = "EXTRACT"
        TRANSFORM = "TRANSFORM"
        LOAD = "LOAD"
        VALIDATE = "VALIDATE"
        COMPLETE = "COMPLETE"
        ERROR = "ERROR"
    
    # Standard messages
    class Message:
        INIT_SUCCESS = "ETL process initialized successfully"
        EXTRACT_START = "Starting data extraction"
        EXTRACT_COMPLETE = "Data extraction completed"
        TRANSFORM_START = "Starting data transformation"
        TRANSFORM_COMPLETE = "Data transformation completed"
        LOAD_START = "Starting data load"
        LOAD_COMPLETE = "Data load completed"
        ETL_COMPLETE = "ETL process completed successfully"
        ETL_ERROR = "ETL process failed"


def create_logger(spark: SparkSession, delta_path: str) -> ETLLogger:
    """
    Factory function to create ETL logger with auto-generated run ID
    
    Args:
        spark: SparkSession instance
        delta_path: Path to Delta Lake table
        
    Returns:
        Configured ETLLogger instance
    """
    etl_run_id = ETLLogger.generate_etl_run_id()
    return ETLLogger(spark, etl_run_id, delta_path)