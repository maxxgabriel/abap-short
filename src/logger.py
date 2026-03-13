"""
Logger module for ETL process.
Provides centralized logging functionality.
"""
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from pyspark.sql import SparkSession


@dataclass
class LogEntry:
    """ETL log entry structure."""
    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""


class ETLLogger:
    """Logger for ETL process."""
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize logger.
        
        Args:
            etl_run_id: Unique ETL run identifier
            spark: Optional SparkSession for database logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries = []
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message.
        
        Args:
            step: Process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        entry = LogEntry(
            log_id=log_id,
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime("%Y-%m-%d"),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self.log_entries.append(entry)
        
        # Console output
        status_symbol = {
            'S': '✓',
            'E': '✗',
            'W': '⚠',
            'I': 'ℹ'
        }.get(status, '•')
        
        print(f"[{entry.execution_time}] {status_symbol} {step}: {message}")
        
        if records_processed > 0:
            print(f"  Records - Processed: {records_processed}, "
                  f"Success: {records_success}, Error: {records_error}")
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run ID."""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries."""
        return self.log_entries
    
    def persist_logs(self, table_name: str = "zetl_log") -> None:
        """
        Persist logs to database table.
        
        Args:
            table_name: Target log table name
        """
        if self.spark and self.log_entries:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType
            
            schema = StructType([
                StructField("log_id", StringType(), False),
                StructField("etl_run_id", StringType(), False),
                StructField("execution_date", StringType(), False),
                StructField("execution_time", StringType(), False),
                StructField("process_step", StringType(), False),
                StructField("status", StringType(), False),
                StructField("records_processed", IntegerType(), False),
                StructField("records_success", IntegerType(), False),
                StructField("records_error", IntegerType(), False),
                StructField("message", StringType(), False)
            ])
            
            # Convert log entries to list of tuples
            data = [
                (
                    entry.log_id,
                    entry.etl_run_id,
                    entry.execution_date,
                    entry.execution_time,
                    entry.process_step,
                    entry.status,
                    entry.records_processed,
                    entry.records_success,
                    entry.records_error,
                    entry.message
                )
                for entry in self.log_entries
            ]
            
            df = self.spark.createDataFrame(data, schema)
            df.write.mode("append").saveAsTable(table_name)