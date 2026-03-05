"""
ETL Logger Module - Converts ABAP logger methods to PySpark DataFrame operations
Writes audit logs to Delta Lake with ZETL_LOG schema mapping
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, current_date, lit
from datetime import datetime
from typing import Optional, Dict, Any
import uuid


class ETLLogger:
    """
    Python logging wrapper that converts ABAP logger methods to PySpark DataFrame operations.
    Writes audit logs to Delta Lake with ZETL_LOG schema mapping.
    """

    # Status codes mapping from ABAP
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    STATUS_NEW = 'N'
    STATUS_PROCESSED = 'P'

    # Process step constants
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    STEP_START = 'START'

    def __init__(self, spark: SparkSession, etl_run_id: str, delta_table_path: str):
        """
        Initialize ETL Logger.

        Args:
            spark: Active SparkSession
            etl_run_id: Unique identifier for this ETL run
            delta_table_path: Path to Delta Lake table for audit logs
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.delta_table_path = delta_table_path
        self._log_buffer = []

    @staticmethod
    def get_schema() -> StructType:
        """
        Returns the ZETL_LOG schema structure matching ABAP table definition.

        Returns:
            StructType schema for audit log table
        """
        return StructType([
            StructField("log_id", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("execution_date", DateType(), nullable=False),
            StructField("execution_time", StringType(), nullable=False),
            StructField("process_step", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("records_processed", IntegerType(), nullable=True),
            StructField("records_success", IntegerType(), nullable=True),
            StructField("records_error", IntegerType(), nullable=True),
            StructField("message", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("created_by", StringType(), nullable=True)
        ])

    def generate_log_id(self) -> str:
        """
        Generate unique log ID similar to ABAP implementation.

        Returns:
            Unique log identifier with LOG prefix
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_suffix = str(uuid.uuid4())[:6]
        return f"LOG{timestamp}{unique_suffix}"

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
        Log a message to the buffer (ABAP-style log_message method).

        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()

        log_entry = {
            "log_id": self.generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now.strftime('%H:%M:%S'),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message[:255] if message else None,  # Truncate to CHAR255
            "created_at": now,
            "created_by": "etl_system"
        }

        self._log_buffer.append(log_entry)

        # Also print to console for immediate visibility
        print(f"[{now.strftime('%H:%M:%S')}] {step} | {status} | {message}")

    def flush_logs(self) -> None:
        """
        Flush buffered logs to Delta Lake table.
        Converts buffer to DataFrame and writes to Delta.
        """
        if not self._log_buffer:
            print("No logs to flush")
            return

        try:
            # Create DataFrame from log buffer
            log_df = self.spark.createDataFrame(self._log_buffer, schema=self.get_schema())

            # Write to Delta Lake in append mode
            log_df.write \
                .format("delta") \
                .mode("append") \
                .save(self.delta_table_path)

            print(f"Flushed {len(self._log_buffer)} log entries to Delta Lake: {self.delta_table_path}")

            # Clear buffer after successful write
            self._log_buffer.clear()

        except Exception as e:
            print(f"Error flushing logs to Delta Lake: {str(e)}")
            raise

    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.

        Returns:
            ETL run identifier
        """
        return self.etl_run_id

    def read_logs(self, filter_conditions: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Read logs from Delta Lake table with optional filtering.

        Args:
            filter_conditions: Dictionary of column:value pairs for filtering

        Returns:
            DataFrame containing filtered log entries
        """
        try:
            df = self.spark.read.format("delta").load(self.delta_table_path)

            if filter_conditions:
                for column, value in filter_conditions.items():
                    df = df.filter(df[column] == value)

            return df

        except Exception as e:
            print(f"Error reading logs from Delta Lake: {str(e)}")
            raise

    def get_run_summary(self) -> DataFrame:
        """
        Get summary statistics for the current ETL run.

        Returns:
            DataFrame with aggregated statistics for this run
        """
        try:
            df = self.read_logs({"etl_run_id": self.etl_run_id})

            summary = df.groupBy("process_step", "status") \
                .agg({
                    "records_processed": "sum",
                    "records_success": "sum",
                    "records_error": "sum"
                }) \
                .orderBy("process_step", "status")

            return summary

        except Exception as e:
            print(f"Error generating run summary: {str(e)}")
            raise

    def display_summary(self) -> None:
        """
        Display ETL run summary (similar to ABAP display_summary).
        """
        print("=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")

        try:
            summary_df = self.get_run_summary()
            summary_df.show(truncate=False)
        except Exception:
            print("Unable to retrieve summary from Delta Lake")

        print("=" * 60)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto-flush logs."""
        if self._log_buffer:
            try:
                self.flush_logs()
            except Exception as e:
                print(f"Warning: Failed to flush logs on exit: {str(e)}")
        return False


def create_etl_run_id() -> str:
    """
    Generate unique ETL run ID with ETL prefix (similar to ABAP generate_etl_run_id).

    Returns:
        Unique ETL run identifier
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"ETL{timestamp}"