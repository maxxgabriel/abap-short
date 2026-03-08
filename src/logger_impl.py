"""
Concrete implementation of ETL Logger
"""
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

from src.logger_interface import (
    IETLLogger,
    LogStatus,
    ProcessStep,
    ETLLoggerProtocol
)


class ETLLogger(IETLLogger):
    """
    Concrete implementation of ETL logger.
    Converted from ABAP class ZCL_ETL_LOGGER.
    """
    
    def __init__(
        self,
        etl_run_id: str,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize the logger
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Optional SparkSession for distributed logging
        """
        self._etl_run_id = etl_run_id
        self._spark = spark
        self._log_entries = []
    
    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log an ETL process message
        
        Args:
            step: The process step being logged
            status: The status of the operation
            message: Descriptive message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Failed records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self._etl_run_id,
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "process_step": step.value,
            "status": status.value,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        self._log_entries.append(log_entry)
        
        # Console output
        self._print_log(log_entry)
        
        # Optionally persist to storage
        if self._spark:
            self._persist_log(log_entry)
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier
        
        Returns:
            ETL run ID string
        """
        return self._etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries for this run
        
        Returns:
            List of log entry dictionaries
        """
        return self._log_entries
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID based on timestamp
        
        Returns:
            Unique log ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp[:14]}"
    
    def _print_log(self, log_entry: dict) -> None:
        """
        Print log entry to console
        
        Args:
            log_entry: Log entry dictionary
        """
        print(
            f"[{log_entry['execution_time']}] "
            f"{log_entry['process_step']:12} "
            f"[{log_entry['status']}] "
            f"{log_entry['message']}"
        )
        
        if log_entry['records_processed'] > 0:
            print(
                f"  -> Processed: {log_entry['records_processed']}, "
                f"Success: {log_entry['records_success']}, "
                f"Errors: {log_entry['records_error']}"
            )
    
    def _persist_log(self, log_entry: dict) -> None:
        """
        Persist log entry to storage (Delta/Parquet)
        
        Args:
            log_entry: Log entry dictionary
        """
        try:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType
            
            schema = StructType([
                StructField("log_id", StringType(), False),
                StructField("etl_run_id", StringType(), False),
                StructField("execution_date", StringType(), False),
                StructField("execution_time", StringType(), False),
                StructField("process_step", StringType(), False),
                StructField("status", StringType(), False),
                StructField("records_processed", IntegerType(), True),
                StructField("records_success", IntegerType(), True),
                StructField("records_error", IntegerType(), True),
                StructField("message", StringType(), True),
                StructField("timestamp", StringType(), False)
            ])
            
            df = self._spark.createDataFrame([log_entry], schema)
            
            # Append to Delta table (or Parquet)
            log_path = "s3://etl-logs/etl_log"
            df.write.format("delta").mode("append").save(log_path)
            
        except Exception as e:
            print(f"Warning: Failed to persist log entry: {str(e)}")


def verify_logger_protocol(logger: object) -> bool:
    """
    Verify that an object implements the ETLLoggerProtocol
    
    Args:
        logger: Object to verify
        
    Returns:
        True if object implements protocol, False otherwise
    """
    return isinstance(logger, ETLLoggerProtocol)