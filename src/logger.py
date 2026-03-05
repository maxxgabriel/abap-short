"""
Unified Logging Framework with Delta Table Persistence
Migrated from ABAP ZCL_ETL_LOGGER to Python with PySpark DataFrame-based logging.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta import DeltaTable
import uuid


@dataclass
class LogEntry:
    """Structure for individual log entries"""
    log_id: str
    etl_run_id: str
    execution_timestamp: datetime
    process_step: str
    status: str
    records_processed: int
    records_success: int
    records_error: int
    message: str
    created_by: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation"""
        data = asdict(self)
        # Convert datetime to string for Spark
        data['execution_timestamp'] = self.execution_timestamp.isoformat()
        return data


class ETLLogger:
    """
    Production-grade ETL logging framework with Delta Lake persistence.
    Migrated from ABAP ZCL_ETL_LOGGER class.
    """
    
    # Status constants (from ABAP gc_status)
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Step constants (from ABAP gc_step)
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("execution_timestamp", TimestampType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), False),
        StructField("records_success", IntegerType(), False),
        StructField("records_error", IntegerType(), False),
        StructField("message", StringType(), True),
        StructField("created_by", StringType(), False)
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        etl_run_id: str,
        delta_table_path: str,
        user: str = "system",
        console_logging: bool = True,
        log_level: str = "INFO"
    ):
        """
        Initialize the ETL logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique ETL run identifier
            delta_table_path: Path to Delta table for log persistence
            user: Username for audit trail
            console_logging: Enable console logging
            log_level: Python logging level
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.delta_table_path = delta_table_path
        self.user = user
        self._log_buffer: List[LogEntry] = []
        
        # Setup Python logging for console output
        self.console_logging = console_logging
        if console_logging:
            logging.basicConfig(
                level=getattr(logging, log_level.upper()),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.logger = logging.getLogger(f"ETLLogger-{etl_run_id}")
        
        # Initialize Delta table if it doesn't exist
        self._initialize_delta_table()
    
    def _initialize_delta_table(self) -> None:
        """Create Delta table if it doesn't exist"""
        try:
            # Check if table exists
            DeltaTable.forPath(self.spark, self.delta_table_path)
        except Exception:
            # Create empty DataFrame with schema and save as Delta
            empty_df = self.spark.createDataFrame([], schema=self.LOG_SCHEMA)
            empty_df.write.format("delta").mode("overwrite").save(self.delta_table_path)
            
            if self.console_logging:
                self.logger.info(f"Initialized Delta table at {self.delta_table_path}")
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID (migrated from ABAP generate_log_id)"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_suffix = str(uuid.uuid4())[:8]
        return f"LOG{timestamp}{unique_suffix}"
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        flush_immediate: bool = False
    ) -> None:
        """
        Log a message to buffer and optionally persist immediately.
        Migrated from ABAP log_message method.
        
        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
            flush_immediate: Write to Delta immediately
        """
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_timestamp=datetime.now(),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_by=self.user
        )
        
        # Add to buffer
        self._log_buffer.append(log_entry)
        
        # Console logging
        if self.console_logging:
            log_level = self._map_status_to_log_level(status)
            formatted_message = (
                f"[{step}] {message} | "
                f"Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Error: {records_error}"
            )
            self.logger.log(log_level, formatted_message)
        
        # Flush to Delta if requested
        if flush_immediate:
            self.flush_to_delta()
    
    def _map_status_to_log_level(self, status: str) -> int:
        """Map ABAP status codes to Python logging levels"""
        mapping = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_INFO: logging.INFO,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_ERROR: logging.ERROR
        }
        return mapping.get(status, logging.INFO)
    
    def flush_to_delta(self) -> None:
        """Persist buffered logs to Delta table"""
        if not self._log_buffer:
            return
        
        try:
            # Convert log entries to list of dicts
            log_dicts = [entry.to_dict() for entry in self._log_buffer]
            
            # Create DataFrame
            log_df = self.spark.createDataFrame(log_dicts, schema=self.LOG_SCHEMA)
            
            # Append to Delta table
            log_df.write.format("delta").mode("append").save(self.delta_table_path)
            
            if self.console_logging:
                self.logger.info(f"Flushed {len(self._log_buffer)} log entries to Delta table")
            
            # Clear buffer
            self._log_buffer.clear()
            
        except Exception as e:
            if self.console_logging:
                self.logger.error(f"Failed to flush logs to Delta: {str(e)}")
            raise
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID (migrated from ABAP method)"""
        return self.etl_run_id
    
    def get_logs_for_run(self, etl_run_id: Optional[str] = None) -> DataFrame:
        """
        Retrieve logs for specific ETL run from Delta table.
        
        Args:
            etl_run_id: ETL run ID to filter (defaults to current run)
            
        Returns:
            DataFrame with filtered logs
        """
        run_id = etl_run_id or self.etl_run_id
        
        delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
        logs_df = delta_table.toDF()
        
        return logs_df.filter(logs_df.etl_run_id == run_id).orderBy("execution_timestamp")
    
    def get_summary_stats(self, etl_run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary statistics for an ETL run.
        
        Args:
            etl_run_id: ETL run ID to analyze (defaults to current run)
            
        Returns:
            Dictionary with summary statistics
        """
        logs_df = self.get_logs_for_run(etl_run_id)
        
        # Collect stats using Spark aggregations
        from pyspark.sql.functions import sum, count, min, max
        
        stats = logs_df.agg(
            count("*").alias("total_log_entries"),
            sum("records_processed").alias("total_processed"),
            sum("records_success").alias("total_success"),
            sum("records_error").alias("total_errors"),
            min("execution_timestamp").alias("start_time"),
            max("execution_timestamp").alias("end_time")
        ).collect()[0]
        
        # Status breakdown
        status_counts = (
            logs_df.groupBy("status")
            .count()
            .rdd.collectAsMap()
        )
        
        return {
            "etl_run_id": etl_run_id or self.etl_run_id,
            "total_log_entries": stats["total_log_entries"],
            "total_processed": stats["total_processed"] or 0,
            "total_success": stats["total_success"] or 0,
            "total_errors": stats["total_errors"] or 0,
            "start_time": stats["start_time"],
            "end_time": stats["end_time"],
            "status_breakdown": status_counts
        }
    
    def close(self) -> None:
        """Flush any remaining logs and close logger"""
        self.flush_to_delta()
        if self.console_logging:
            self.logger.info(f"Logger closed for ETL run {self.etl_run_id}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures logs are flushed"""
        self.close()


class LoggerFactory:
    """Factory for creating ETL logger instances"""
    
    @staticmethod
    def create_logger(
        spark: SparkSession,
        etl_run_id: str,
        config: Dict[str, Any]
    ) -> ETLLogger:
        """
        Create an ETL logger instance from configuration.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique ETL run identifier
            config: Configuration dictionary
            
        Returns:
            Configured ETLLogger instance
        """
        return ETLLogger(
            spark=spark,
            etl_run_id=etl_run_id,
            delta_table_path=config.get("delta_table_path"),
            user=config.get("user", "system"),
            console_logging=config.get("console_logging", True),
            log_level=config.get("log_level", "INFO")
        )