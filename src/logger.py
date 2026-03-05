"""
ETL Logger Module
Provides logging functionality with DataFrame persistence for ETL processes.
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


@dataclass
class LogEntry:
    """Dataclass representing a single log entry."""
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
    created_at: Optional[str] = None
    created_by: Optional[str] = None

    def __post_init__(self):
        """Set default values for created_at and created_by if not provided."""
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.created_by is None:
            self.created_by = "etl_system"


class ETLLogger:
    """
    ETL Logger class that handles logging with DataFrame persistence.
    Maintains compatibility with ABAP logging structure while using Python logging.
    """

    # Status constants
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'

    # Step constants
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'

    # Log schema for DataFrame operations
    LOG_SCHEMA = StructType([
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

    def __init__(self, etl_run_id: str, spark: SparkSession, log_table_path: Optional[str] = None):
        """
        Initialize the ETL Logger.

        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession instance for DataFrame operations
            log_table_path: Optional path to persist logs (Delta/Parquet table)
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_table_path = log_table_path
        self._log_entries: List[LogEntry] = []
        self._log_counter = 0

        # Initialize Python logger for console output
        self._setup_python_logger()

        # Log initialization
        self.log_message(
            step=self.STEP_INIT,
            status=self.STATUS_SUCCESS,
            message=f"ETL Logger initialized with run ID: {etl_run_id}"
        )

    def _setup_python_logger(self):
        """Configure Python logging for console output."""
        self.logger = logging.getLogger(f"ETLLogger_{self.etl_run_id}")
        self.logger.setLevel(logging.INFO)

        # Avoid duplicate handlers
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _generate_log_id(self) -> str:
        """
        Generate a unique log ID.

        Returns:
            Unique log ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        self._log_counter += 1
        return f"LOG{timestamp}{self._log_counter:04d}"

    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """
        Log a message with optional record statistics.

        Args:
            step: ETL process step
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
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
            message=message
        )

        # Store log entry
        self._log_entries.append(log_entry)

        # Log to Python logger
        log_level = self._get_log_level(status)
        log_msg = f"[{step}] {message}"
        if records_processed > 0:
            log_msg += f" (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"

        self.logger.log(log_level, log_msg)

    def _get_log_level(self, status: str) -> int:
        """
        Convert status code to Python logging level.

        Args:
            status: Status code

        Returns:
            Python logging level
        """
        status_map = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_ERROR: logging.ERROR,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_INFO: logging.INFO
        }
        return status_map.get(status, logging.INFO)

    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.

        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def get_log_entries(self) -> List[LogEntry]:
        """
        Get all log entries for this ETL run.

        Returns:
            List of LogEntry objects
        """
        return self._log_entries.copy()

    def to_dataframe(self) -> DataFrame:
        """
        Convert log entries to Spark DataFrame.

        Returns:
            DataFrame containing all log entries
        """
        if not self._log_entries:
            return self.spark.createDataFrame([], schema=self.LOG_SCHEMA)

        log_dicts = [asdict(entry) for entry in self._log_entries]
        return self.spark.createDataFrame(log_dicts, schema=self.LOG_SCHEMA)

    def persist_logs(self, mode: str = "append") -> bool:
        """
        Persist log entries to configured storage.

        Args:
            mode: Write mode (append, overwrite, etc.)

        Returns:
            True if successful, False otherwise
        """
        if not self.log_table_path:
            self.logger.warning("No log table path configured. Logs not persisted.")
            return False

        try:
            log_df = self.to_dataframe()

            if log_df.count() == 0:
                self.logger.warning("No log entries to persist.")
                return True

            # Write to configured storage
            log_df.write.mode(mode).format("delta").save(self.log_table_path)

            self.logger.info(f"Successfully persisted {log_df.count()} log entries to {self.log_table_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            return False

    def get_summary_statistics(self) -> dict:
        """
        Get summary statistics for this ETL run.

        Returns:
            Dictionary containing summary statistics
        """
        log_df = self.to_dataframe()

        if log_df.count() == 0:
            return {
                "total_logs": 0,
                "success_count": 0,
                "error_count": 0,
                "warning_count": 0,
                "total_records_processed": 0,
                "total_records_success": 0,
                "total_records_error": 0
            }

        # Aggregate statistics using DataFrame operations
        from pyspark.sql.functions import col, sum as spark_sum, count, when

        stats_df = log_df.agg(
            count("*").alias("total_logs"),
            spark_sum(when(col("status") == self.STATUS_SUCCESS, 1).otherwise(0)).alias("success_count"),
            spark_sum(when(col("status") == self.STATUS_ERROR, 1).otherwise(0)).alias("error_count"),
            spark_sum(when(col("status") == self.STATUS_WARNING, 1).otherwise(0)).alias("warning_count"),
            spark_sum("records_processed").alias("total_records_processed"),
            spark_sum("records_success").alias("total_records_success"),
            spark_sum("records_error").alias("total_records_error")
        )

        return stats_df.first().asDict()

    def display_summary(self):
        """Display a summary of the ETL run logs."""
        stats = self.get_summary_statistics()

        print("\n" + "=" * 70)
        print("ETL Logging Summary")
        print("=" * 70)
        print(f"ETL Run ID:              {self.etl_run_id}")
        print(f"Total Log Entries:       {stats['total_logs']}")
        print(f"Success Entries:         {stats['success_count']}")
        print(f"Error Entries:           {stats['error_count']}")
        print(f"Warning Entries:         {stats['warning_count']}")
        print(f"Total Records Processed: {stats['total_records_processed']}")
        print(f"Total Records Success:   {stats['total_records_success']}")
        print(f"Total Records Error:     {stats['total_records_error']}")
        print("=" * 70 + "\n")

    def close(self):
        """
        Close the logger and persist any remaining logs.
        """
        self.log_message(
            step=self.STEP_COMPLETE,
            status=self.STATUS_INFO,
            message="Logger closing - final log entry"
        )
        self.persist_logs()


def create_logger(etl_run_id: str, spark: SparkSession, config: dict) -> ETLLogger:
    """
    Factory function to create an ETL Logger instance.

    Args:
        etl_run_id: Unique identifier for the ETL run
        spark: SparkSession instance
        config: Configuration dictionary containing log_table_path

    Returns:
        Configured ETLLogger instance
    """
    log_table_path = config.get('log_table_path')
    return ETLLogger(etl_run_id, spark, log_table_path)