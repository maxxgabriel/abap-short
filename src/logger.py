"""
ETL Logger with Delta Lake Audit Trail
Converts ABAP logger methods to PySpark DataFrame operations
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, current_date, lit, col
from delta import DeltaTable
from datetime import datetime
from typing import Optional, Dict, Any
import uuid


class ETLLogger:
    """
    PySpark implementation of ABAP ZCL_ETL_LOGGER
    Writes audit logs to Delta Lake with ZETL_LOG schema mapping
    """
    
    # Schema mapping from ABAP ZETL_LOG table
    LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("execution_date", DateType(), False),
        StructField("execution_time", StringType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), False),
        StructField("records_success", IntegerType(), False),
        StructField("records_error", IntegerType(), False),
        StructField("message", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("created_by", StringType(), False),
    ])
    
    # Status codes from ZCL_ETL_CONSTANTS
    STATUS_SUCCESS = "S"
    STATUS_ERROR = "E"
    STATUS_WARNING = "W"
    STATUS_INFO = "I"
    
    # Process steps from ZCL_ETL_CONSTANTS
    STEP_INIT = "INIT"
    STEP_EXTRACT = "EXTRACT"
    STEP_TRANSFORM = "TRANSFORM"
    STEP_LOAD = "LOAD"
    STEP_VALIDATE = "VALIDATE"
    STEP_COMPLETE = "COMPLETE"
    STEP_ERROR = "ERROR"
    
    def __init__(self, spark: SparkSession, etl_run_id: str, 
                 delta_path: str, username: str = "system"):
        """
        Initialize ETL Logger
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique ETL run identifier
            delta_path: Path to Delta Lake audit log table
            username: Username for created_by field
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.delta_path = delta_path
        self.username = username
        self._ensure_delta_table()
        
    def _ensure_delta_table(self) -> None:
        """Ensure Delta Lake table exists, create if not"""
        try:
            # Check if Delta table exists
            DeltaTable.forPath(self.spark, self.delta_path)
        except Exception:
            # Create empty DataFrame with schema and write as Delta
            empty_df = self.spark.createDataFrame([], schema=self.LOG_SCHEMA)
            empty_df.write.format("delta").mode("overwrite").save(self.delta_path)
            
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID
        Mimics ABAP generate_log_id method
        
        Returns:
            Unique log ID with LOG prefix
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
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
        Log a message to Delta Lake audit trail
        Converts ABAP log_message method to PySpark DataFrame operation
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_id = self._generate_log_id()
        execution_time = datetime.now().strftime("%H:%M:%S")
        
        # Create log entry as DataFrame
        log_data = [(
            log_id,
            self.etl_run_id,
            datetime.now().date(),
            execution_time,
            step,
            status,
            records_processed,
            records_success,
            records_error,
            message,
            datetime.now(),
            self.username
        )]
        
        log_df = self.spark.createDataFrame(log_data, schema=self.LOG_SCHEMA)
        
        # Append to Delta Lake
        log_df.write.format("delta").mode("append").save(self.delta_path)
        
        # Console output (matching ABAP WRITE statement)
        print(f"{execution_time} | {step:15} | {status} | {message}")
    
    def get_etl_run_id(self) -> str:
        """
        Get current ETL run ID
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def get_logs(self, filter_step: Optional[str] = None, 
                 filter_status: Optional[str] = None) -> DataFrame:
        """
        Retrieve logs from Delta Lake
        
        Args:
            filter_step: Optional step filter
            filter_status: Optional status filter
            
        Returns:
            DataFrame with filtered logs
        """
        df = self.spark.read.format("delta").load(self.delta_path)
        df = df.filter(col("etl_run_id") == self.etl_run_id)
        
        if filter_step:
            df = df.filter(col("process_step") == filter_step)
        if filter_status:
            df = df.filter(col("status") == filter_status)
            
        return df.orderBy("created_at")
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Get summary statistics for current ETL run
        
        Returns:
            Dictionary with summary statistics
        """
        df = self.get_logs()
        
        # Calculate aggregations
        total_processed = df.agg({"records_processed": "sum"}).collect()[0][0] or 0
        total_success = df.agg({"records_success": "sum"}).collect()[0][0] or 0
        total_error = df.agg({"records_error": "sum"}).collect()[0][0] or 0
        
        error_count = df.filter(col("status") == self.STATUS_ERROR).count()
        warning_count = df.filter(col("status") == self.STATUS_WARNING).count()
        
        start_time = df.agg({"created_at": "min"}).collect()[0][0]
        end_time = df.agg({"created_at": "max"}).collect()[0][0]
        
        duration = None
        if start_time and end_time:
            duration = (end_time - start_time).total_seconds()
        
        return {
            "etl_run_id": self.etl_run_id,
            "total_records_processed": total_processed,
            "total_records_success": total_success,
            "total_records_error": total_error,
            "error_count": error_count,
            "warning_count": warning_count,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration
        }
    
    def display_summary(self) -> None:
        """Display ETL run summary (mimics ABAP display_summary)"""
        stats = self.get_summary_statistics()
        
        print("\n" + "=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:         {stats['etl_run_id']}")
        print(f"Start Time:         {stats['start_time']}")
        print(f"End Time:           {stats['end_time']}")
        if stats['duration_seconds']:
            print(f"Duration:           {stats['duration_seconds']:.2f} seconds")
        print(f"Records Processed:  {stats['total_records_processed']}")
        print(f"Records Success:    {stats['total_records_success']}")
        print(f"Records Error:      {stats['total_records_error']}")
        print(f"Errors:             {stats['error_count']}")
        print(f"Warnings:           {stats['warning_count']}")
        print("=" * 70 + "\n")


def generate_etl_run_id() -> str:
    """
    Generate unique ETL run ID
    Mimics ABAP generate_etl_run_id method
    
    Returns:
        Unique ETL run ID with ETL prefix
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"ETL{timestamp}"


def create_logger(spark: SparkSession, delta_path: str, 
                  etl_run_id: Optional[str] = None,
                  username: str = "system") -> ETLLogger:
    """
    Factory function to create ETL Logger instance
    
    Args:
        spark: SparkSession instance
        delta_path: Path to Delta Lake audit log table
        etl_run_id: Optional ETL run ID (generated if not provided)
        username: Username for created_by field
        
    Returns:
        ETLLogger instance
    """
    if etl_run_id is None:
        etl_run_id = generate_etl_run_id()
    
    return ETLLogger(spark, etl_run_id, delta_path, username)