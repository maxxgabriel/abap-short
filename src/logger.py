"""
ETL Logger Module
Implements logging functionality with DataFrame persistence for ETL processes.
Migrated from ABAP ZCL_ETL_LOGGER class.
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


@dataclass
class LogEntry:
    """
    Data class representing a single ETL log entry.
    Corresponds to ABAP ty_log_entry structure.
    """
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
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = field(default_factory=lambda: "system")

    def to_dict(self):
        """Convert log entry to dictionary for DataFrame creation."""
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat()
        return data


class ETLLogger:
    """
    ETL Logger class for logging ETL process execution.
    Provides both console logging and DataFrame-based persistence.
    Migrated from ABAP ZCL_ETL_LOGGER class.
    """
    
    # Status constants (from ABAP gc_status)
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    STATUS_NEW = 'N'
    STATUS_PROCESSED = 'P'
    
    # Process step constants (from ABAP gc_step)
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, spark: SparkSession, log_level: str = "INFO"):
        """
        Initialize ETL Logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession instance for DataFrame operations
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries: List[LogEntry] = []
        
        # Configure Python logging
        self.logger = logging.getLogger(f"ETLLogger_{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_level.upper()))
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        Corresponds to ABAP generate_log_id method.
        
        Returns:
            Unique log ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp[:20]}"
    
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
        Log a message with statistics.
        Corresponds to ABAP log_message method.
        
        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        now = datetime.now()
        
        # Create log entry
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime("%Y-%m-%d"),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_at=now
        )
        
        # Store log entry
        self.log_entries.append(log_entry)
        
        # Console logging
        log_msg = (
            f"[{step}] {message} | "
            f"Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error}"
        )
        
        if status == self.STATUS_ERROR:
            self.logger.error(log_msg)
        elif status == self.STATUS_WARNING:
            self.logger.warning(log_msg)
        elif status == self.STATUS_INFO:
            self.logger.info(log_msg)
        else:
            self.logger.info(log_msg)
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        Corresponds to ABAP get_etl_run_id method.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def get_log_schema(self) -> StructType:
        """
        Get the schema for log DataFrame.
        
        Returns:
            StructType schema for log entries
        """
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", StringType(), False),
            StructField("execution_time", StringType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), False),
            StructField("records_success", IntegerType(), False),
            StructField("records_error", IntegerType(), False),
            StructField("message", StringType(), True),
            StructField("created_at", StringType(), False),
            StructField("created_by", StringType(), False)
        ])
    
    def get_logs_as_dataframe(self) -> DataFrame:
        """
        Convert accumulated log entries to Spark DataFrame.
        
        Returns:
            DataFrame containing all log entries
        """
        if not self.log_entries:
            # Return empty DataFrame with schema
            return self.spark.createDataFrame([], schema=self.get_log_schema())
        
        # Convert log entries to list of dictionaries
        log_data = [entry.to_dict() for entry in self.log_entries]
        
        # Create DataFrame
        return self.spark.createDataFrame(log_data, schema=self.get_log_schema())
    
    def persist_logs(
        self,
        target_path: str,
        mode: str = "append",
        format: str = "parquet"
    ) -> None:
        """
        Persist log entries to storage using DataFrame operations.
        
        Args:
            target_path: Target path for log storage
            mode: Write mode (append, overwrite, etc.)
            format: Output format (parquet, delta, csv, etc.)
        """
        try:
            df = self.get_logs_as_dataframe()
            
            if df.count() > 0:
                df.write.mode(mode).format(format).save(target_path)
                self.logger.info(f"Persisted {df.count()} log entries to {target_path}")
            else:
                self.logger.warning("No log entries to persist")
                
        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            raise
    
    def get_summary_statistics(self) -> dict:
        """
        Get summary statistics from log entries.
        
        Returns:
            Dictionary containing aggregated statistics
        """
        if not self.log_entries:
            return {
                "total_logs": 0,
                "errors": 0,
                "warnings": 0,
                "success": 0,
                "total_records_processed": 0,
                "total_records_success": 0,
                "total_records_error": 0
            }
        
        df = self.get_logs_as_dataframe()
        
        # Calculate statistics using DataFrame operations
        from pyspark.sql.functions import sum as spark_sum, count, col
        
        stats_df = df.agg(
            count("*").alias("total_logs"),
            spark_sum(
                (col("status") == self.STATUS_ERROR).cast("int")
            ).alias("errors"),
            spark_sum(
                (col("status") == self.STATUS_WARNING).cast("int")
            ).alias("warnings"),
            spark_sum(
                (col("status") == self.STATUS_SUCCESS).cast("int")
            ).alias("success"),
            spark_sum("records_processed").alias("total_records_processed"),
            spark_sum("records_success").alias("total_records_success"),
            spark_sum("records_error").alias("total_records_error")
        )
        
        stats_row = stats_df.first()
        
        return {
            "total_logs": stats_row["total_logs"],
            "errors": stats_row["errors"] or 0,
            "warnings": stats_row["warnings"] or 0,
            "success": stats_row["success"] or 0,
            "total_records_processed": stats_row["total_records_processed"] or 0,
            "total_records_success": stats_row["total_records_success"] or 0,
            "total_records_error": stats_row["total_records_error"] or 0
        }
    
    def clear_logs(self) -> None:
        """Clear accumulated log entries from memory."""
        self.log_entries.clear()
        self.logger.info("Log entries cleared from memory")


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate a unique ETL run ID.
    Corresponds to ABAP generate_etl_run_id method.
    
    Args:
        prefix: Prefix for the run ID
        
    Returns:
        Unique ETL run ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8]
    return f"{prefix}{timestamp}{unique_suffix}"