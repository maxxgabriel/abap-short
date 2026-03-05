"""
Unified Logging Framework with Delta Table Persistence
Migrated from ABAP ZCL_ETL_LOGGER
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DateType
)
from delta import DeltaTable
import uuid


class ETLLogger:
    """
    Unified logging framework that combines Python logging with Delta table persistence.
    Provides structured logging with ETL context and automatic persistence to Delta Lake.
    """
    
    # Status constants (migrated from ABAP)
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
    
    def __init__(
        self,
        spark: SparkSession,
        etl_run_id: str,
        delta_table_path: str,
        log_level: int = logging.INFO,
        logger_name: str = "etl_logger"
    ):
        """
        Initialize the unified logging framework.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique identifier for this ETL run
            delta_table_path: Path to Delta table for log persistence
            log_level: Python logging level
            logger_name: Name for the Python logger
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.delta_table_path = delta_table_path
        
        # Initialize Python logger
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        
        # Configure console handler if not already configured
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Initialize Delta table
        self._initialize_delta_table()
        
        # In-memory buffer for batch writing
        self._log_buffer = []
        
    def _initialize_delta_table(self) -> None:
        """Initialize Delta table for log persistence if it doesn't exist."""
        schema = self._get_log_schema()
        
        try:
            # Check if table exists
            DeltaTable.forPath(self.spark, self.delta_table_path)
            self.logger.info(f"Using existing Delta table at {self.delta_table_path}")
        except Exception:
            # Create new Delta table
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.write.format("delta").save(self.delta_table_path)
            self.logger.info(f"Created new Delta table at {self.delta_table_path}")
    
    @staticmethod
    def _get_log_schema() -> StructType:
        """
        Define schema for log Delta table.
        Migrated from ABAP ty_log_entry structure.
        """
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("created_by", StringType(), True),
            StructField("additional_context", StringType(), True)  # JSON string for extra data
        ])
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        Migrated from ABAP generate_log_id method.
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"LOG_{timestamp}_{unique_id}"
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        additional_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log message to both Python logger and Delta table.
        Migrated from ABAP log_message method.
        
        Args:
            step: Process step (e.g., EXTRACT, TRANSFORM, LOAD)
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
            additional_context: Additional context as dictionary
        """
        now = datetime.now()
        
        # Log to Python logger
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{self.etl_run_id}] [{step}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
        
        # Create log entry for Delta table
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now,
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message[:255],  # Truncate to match ABAP char255
            "created_at": now,
            "created_by": self._get_current_user(),
            "additional_context": self._serialize_context(additional_context)
        }
        
        # Add to buffer
        self._log_buffer.append(log_entry)
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to Python logging level."""
        mapping = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_ERROR: logging.ERROR,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_INFO: logging.INFO
        }
        return mapping.get(status, logging.INFO)
    
    def _get_current_user(self) -> str:
        """Get current user (placeholder for actual user retrieval)."""
        import getpass
        try:
            return getpass.getuser()
        except Exception:
            return "system"
    
    def _serialize_context(self, context: Optional[Dict[str, Any]]) -> Optional[str]:
        """Serialize additional context to JSON string."""
        if context is None:
            return None
        import json
        try:
            return json.dumps(context)
        except Exception as e:
            self.logger.warning(f"Failed to serialize context: {e}")
            return str(context)
    
    def flush_logs(self) -> None:
        """
        Flush buffered logs to Delta table.
        Should be called periodically or at the end of ETL process.
        """
        if not self._log_buffer:
            return
        
        try:
            # Create DataFrame from buffer
            log_df = self.spark.createDataFrame(
                self._log_buffer,
                schema=self._get_log_schema()
            )
            
            # Append to Delta table
            log_df.write.format("delta").mode("append").save(self.delta_table_path)
            
            count = len(self._log_buffer)
            self.logger.info(f"Flushed {count} log entries to Delta table")
            
            # Clear buffer
            self._log_buffer = []
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs to Delta table: {e}")
            # Keep logs in buffer for retry
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        Migrated from ABAP get_etl_run_id method.
        """
        return self.etl_run_id
    
    def query_logs(
        self,
        etl_run_id: Optional[str] = None,
        process_step: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> DataFrame:
        """
        Query logs from Delta table with filters.
        
        Args:
            etl_run_id: Filter by ETL run ID
            process_step: Filter by process step
            status: Filter by status
            start_date: Filter by start date
            end_date: Filter by end date
            
        Returns:
            DataFrame with filtered logs
        """
        df = self.spark.read.format("delta").load(self.delta_table_path)
        
        if etl_run_id:
            df = df.filter(df.etl_run_id == etl_run_id)
        if process_step:
            df = df.filter(df.process_step == process_step)
        if status:
            df = df.filter(df.status == status)
        if start_date:
            df = df.filter(df.execution_date >= start_date.date())
        if end_date:
            df = df.filter(df.execution_date <= end_date.date())
        
        return df.orderBy("execution_time")
    
    def get_run_summary(self, etl_run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary statistics for an ETL run.
        
        Args:
            etl_run_id: ETL run ID to summarize (defaults to current run)
            
        Returns:
            Dictionary with summary statistics
        """
        run_id = etl_run_id or self.etl_run_id
        
        df = self.spark.read.format("delta").load(self.delta_table_path)
        df = df.filter(df.etl_run_id == run_id)
        
        from pyspark.sql import functions as F
        
        summary = df.groupBy().agg(
            F.sum("records_processed").alias("total_processed"),
            F.sum("records_success").alias("total_success"),
            F.sum("records_error").alias("total_error"),
            F.count("*").alias("log_entries"),
            F.sum(F.when(F.col("status") == self.STATUS_ERROR, 1).otherwise(0)).alias("error_count"),
            F.sum(F.when(F.col("status") == self.STATUS_WARNING, 1).otherwise(0)).alias("warning_count"),
            F.min("execution_time").alias("start_time"),
            F.max("execution_time").alias("end_time")
        ).collect()
        
        if summary:
            row = summary[0]
            return {
                "etl_run_id": run_id,
                "total_processed": row.total_processed or 0,
                "total_success": row.total_success or 0,
                "total_error": row.total_error or 0,
                "log_entries": row.log_entries,
                "error_count": row.error_count,
                "warning_count": row.warning_count,
                "start_time": row.start_time,
                "end_time": row.end_time,
                "duration_seconds": (
                    (row.end_time - row.start_time).total_seconds()
                    if row.end_time and row.start_time else 0
                )
            }
        
        return {"etl_run_id": run_id, "message": "No logs found"}
    
    def close(self) -> None:
        """
        Close logger and flush any remaining logs.
        Should be called at the end of ETL process.
        """
        self.flush_logs()
        self.logger.info(f"ETL Logger closed for run {self.etl_run_id}")


class LoggerFactory:
    """Factory class for creating ETL loggers with consistent configuration."""
    
    @staticmethod
    def create_logger(
        spark: SparkSession,
        delta_table_path: str,
        etl_run_id: Optional[str] = None,
        log_level: int = logging.INFO
    ) -> ETLLogger:
        """
        Create a new ETL logger instance.
        
        Args:
            spark: SparkSession instance
            delta_table_path: Path to Delta table
            etl_run_id: Optional ETL run ID (generated if not provided)
            log_level: Python logging level
            
        Returns:
            ETLLogger instance
        """
        if etl_run_id is None:
            etl_run_id = LoggerFactory.generate_etl_run_id()
        
        return ETLLogger(
            spark=spark,
            etl_run_id=etl_run_id,
            delta_table_path=delta_table_path,
            log_level=log_level
        )
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """
        Generate unique ETL run ID.
        Migrated from ABAP generate_etl_run_id method.
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL_{timestamp}_{unique_id}"