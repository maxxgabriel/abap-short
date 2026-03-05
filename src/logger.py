"""
ETL Logger Module - Converts ABAP logging to PySpark DataFrame operations
with Delta Lake audit trail support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from typing import Optional, Dict, Any
import uuid


class ETLLogger:
    """
    Python logging wrapper that converts ABAP logger methods to PySpark DataFrame
    operations, writing audit logs to Delta Lake with ZETL_LOG schema mapping.
    """
    
    # Status codes mapping from ABAP
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Process steps mapping from ABAP
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, spark: SparkSession, etl_run_id: str, delta_table_path: str):
        """
        Initialize ETL Logger with SparkSession and Delta Lake configuration.
        
        Args:
            spark: Active SparkSession instance
            etl_run_id: Unique identifier for this ETL run
            delta_table_path: Path to Delta Lake table for audit logs
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.delta_table_path = delta_table_path
        self._schema = self._create_log_schema()
        self._initialize_delta_table()
    
    def _create_log_schema(self) -> StructType:
        """
        Create schema matching ZETL_LOG ABAP table structure.
        
        Returns:
            StructType defining the log entry schema
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
    
    def _initialize_delta_table(self) -> None:
        """
        Initialize Delta Lake table if it doesn't exist.
        Creates table with proper schema and properties.
        """
        try:
            # Check if table exists
            self.spark.read.format("delta").load(self.delta_table_path)
        except Exception:
            # Create empty DataFrame with schema and write as Delta table
            empty_df = self.spark.createDataFrame([], self._schema)
            empty_df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(self.delta_table_path)
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID similar to ABAP implementation.
        
        Returns:
            Unique log identifier with LOG prefix
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"LOG{timestamp}{unique_id}"
    
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
        Log a message to Delta Lake audit trail (mirrors ABAP log_message method).
        
        Args:
            process_step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        now = datetime.now()
        
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now.strftime("%H:%M:%S"),
            "process_step": process_step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message[:255],  # Truncate to match ABAP char255
            "created_at": now,
            "created_by": self._get_current_user()
        }
        
        # Create DataFrame from log entry
        log_df = self.spark.createDataFrame([log_entry], self._schema)
        
        # Append to Delta Lake table
        log_df.write.format("delta") \
            .mode("append") \
            .save(self.delta_table_path)
        
        # Also print to console for real-time monitoring
        self._print_log_entry(log_entry)
    
    def _print_log_entry(self, log_entry: Dict[str, Any]) -> None:
        """
        Print log entry to console (mirrors ABAP WRITE statement).
        
        Args:
            log_entry: Dictionary containing log entry data
        """
        status_symbol = {
            self.STATUS_SUCCESS: '✓',
            self.STATUS_ERROR: '✗',
            self.STATUS_WARNING: '⚠',
            self.STATUS_INFO: 'ℹ'
        }.get(log_entry['status'], '•')
        
        print(f"[{log_entry['execution_time']}] {status_symbol} "
              f"{log_entry['process_step']:12} | {log_entry['message']}")
        
        if log_entry['records_processed'] > 0:
            print(f"{'':24} | Processed: {log_entry['records_processed']}, "
                  f"Success: {log_entry['records_success']}, "
                  f"Errors: {log_entry['records_error']}")
    
    def _get_current_user(self) -> str:
        """
        Get current user executing the ETL process.
        
        Returns:
            Username or system identifier
        """
        import getpass
        try:
            return getpass.getuser()
        except Exception:
            return "SYSTEM"
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID (mirrors ABAP get_etl_run_id method).
        
        Returns:
            Current ETL run identifier
        """
        return self.etl_run_id
    
    def get_logs_as_dataframe(
        self,
        filter_step: Optional[str] = None,
        filter_status: Optional[str] = None
    ) -> DataFrame:
        """
        Retrieve logs as DataFrame for analysis or reporting.
        
        Args:
            filter_step: Optional process step filter
            filter_status: Optional status filter
            
        Returns:
            DataFrame containing filtered log entries
        """
        df = self.spark.read.format("delta").load(self.delta_table_path)
        
        # Filter by ETL run ID
        df = df.filter(df.etl_run_id == self.etl_run_id)
        
        # Apply optional filters
        if filter_step:
            df = df.filter(df.process_step == filter_step)
        if filter_status:
            df = df.filter(df.status == filter_status)
        
        return df.orderBy("created_at")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics for current ETL run.
        
        Returns:
            Dictionary containing ETL run statistics
        """
        logs_df = self.get_logs_as_dataframe()
        
        from pyspark.sql.functions import sum as spark_sum, count, min as spark_min, max as spark_max
        
        stats = logs_df.agg(
            spark_sum("records_processed").alias("total_processed"),
            spark_sum("records_success").alias("total_success"),
            spark_sum("records_error").alias("total_errors"),
            count("*").alias("total_log_entries"),
            spark_min("created_at").alias("start_time"),
            spark_max("created_at").alias("end_time")
        ).collect()[0]
        
        return {
            "etl_run_id": self.etl_run_id,
            "total_processed": stats["total_processed"] or 0,
            "total_success": stats["total_success"] or 0,
            "total_errors": stats["total_errors"] or 0,
            "total_log_entries": stats["total_log_entries"],
            "start_time": stats["start_time"],
            "end_time": stats["end_time"]
        }
    
    def display_summary(self) -> None:
        """
        Display ETL run summary (mirrors ABAP display_summary method).
        """
        stats = self.get_statistics()
        
        print("\n" + "=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:       {stats['etl_run_id']}")
        print(f"Start Time:       {stats['start_time']}")
        print(f"End Time:         {stats['end_time']}")
        
        if stats['start_time'] and stats['end_time']:
            duration = (stats['end_time'] - stats['start_time']).total_seconds()
            print(f"Duration:         {duration:.2f} seconds")
        
        print(f"\nRecords Processed: {stats['total_processed']}")
        print(f"Records Success:   {stats['total_success']}")
        print(f"Records Errors:    {stats['total_errors']}")
        print(f"Log Entries:       {stats['total_log_entries']}")
        print("=" * 70 + "\n")
    
    def get_error_logs(self) -> DataFrame:
        """
        Get all error log entries for troubleshooting.
        
        Returns:
            DataFrame containing only error logs
        """
        return self.get_logs_as_dataframe(filter_status=self.STATUS_ERROR)
    
    def compact_delta_table(self) -> None:
        """
        Optimize Delta Lake table by running compaction.
        Should be called periodically for maintenance.
        """
        try:
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
            delta_table.optimize().executeCompaction()
            
            self.log_message(
                process_step="MAINTENANCE",
                status=self.STATUS_SUCCESS,
                message="Delta table compaction completed successfully"
            )
        except Exception as e:
            self.log_message(
                process_step="MAINTENANCE",
                status=self.STATUS_ERROR,
                message=f"Delta table compaction failed: {str(e)}"
            )


def create_etl_logger(
    spark: SparkSession,
    delta_table_path: str,
    etl_run_prefix: str = "ETL"
) -> ETLLogger:
    """
    Factory function to create ETLLogger instance with generated run ID.
    
    Args:
        spark: Active SparkSession
        delta_table_path: Path to Delta Lake audit log table
        etl_run_prefix: Prefix for ETL run ID
        
    Returns:
        Configured ETLLogger instance
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    etl_run_id = f"{etl_run_prefix}{timestamp}"
    
    return ETLLogger(
        spark=spark,
        etl_run_id=etl_run_id,
        delta_table_path=delta_table_path
    )