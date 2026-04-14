"""
Advanced log management utilities for ETL processes.
Provides log querying, aggregation, and reporting capabilities.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class LogManager:
    """
    Manages ETL logs with querying and reporting capabilities.
    """
    
    def __init__(self, spark: SparkSession, log_table_path: str):
        """
        Initialize Log Manager.
        
        Args:
            spark: Active SparkSession
            log_table_path: Path to log table storage
        """
        self.spark = spark
        self.log_table_path = log_table_path
    
    def get_logs_by_run_id(self, etl_run_id: str) -> DataFrame:
        """
        Retrieve all logs for a specific ETL run.
        
        Args:
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame containing logs for the specified run
        """
        return self.spark.read \
            .format("delta") \
            .load(self.log_table_path) \
            .filter(F.col("etl_run_id") == etl_run_id) \
            .orderBy("created_at")
    
    def get_logs_by_date_range(
        self,
        start_date: str,
        end_date: str
    ) -> DataFrame:
        """
        Retrieve logs within a date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame containing logs in the date range
        """
        return self.spark.read \
            .format("delta") \
            .load(self.log_table_path) \
            .filter(
                (F.col("execution_date") >= start_date) &
                (F.col("execution_date") <= end_date)
            ) \
            .orderBy("execution_date", "created_at")
    
    def get_error_logs(
        self,
        days_back: int = 7,
        limit: Optional[int] = None
    ) -> DataFrame:
        """
        Retrieve error logs from recent days.
        
        Args:
            days_back: Number of days to look back
            limit: Maximum number of records to return
            
        Returns:
            DataFrame containing error logs
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        df = self.spark.read \
            .format("delta") \
            .load(self.log_table_path) \
            .filter(
                (F.col("status") == "E") &
                (F.col("execution_date") >= cutoff_date)
            ) \
            .orderBy(F.col("created_at").desc())
        
        if limit:
            df = df.limit(limit)
        
        return df
    
    def get_run_summary(self, etl_run_id: str) -> Dict[str, Any]:
        """
        Generate summary statistics for an ETL run.
        
        Args:
            etl_run_id: ETL run identifier
            
        Returns:
            Dictionary containing run summary statistics
        """
        logs_df = self.get_logs_by_run_id(etl_run_id)
        
        if logs_df.count() == 0:
            return {"error": "No logs found for run ID"}
        
        # Aggregate statistics
        summary = logs_df.agg(
            F.min("created_at").alias("start_time"),
            F.max("created_at").alias("end_time"),
            F.sum("records_processed").alias("total_processed"),
            F.sum("records_success").alias("total_success"),
            F.sum("records_error").alias("total_errors"),
            F.count(F.when(F.col("status") == "E", 1)).alias("error_count"),
            F.count(F.when(F.col("status") == "W", 1)).alias("warning_count")
        ).collect()[0]
        
        # Calculate duration
        start_time = summary["start_time"]
        end_time = summary["end_time"]
        duration = (end_time - start_time).total_seconds() if start_time and end_time else 0
        
        # Get step breakdown
        step_breakdown = logs_df.groupBy("process_step") \
            .agg(
                F.sum("records_processed").alias("records"),
                F.count("*").alias("log_entries")
            ) \
            .collect()
        
        return {
            "etl_run_id": etl_run_id,
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None,
            "duration_seconds": duration,
            "total_processed": summary["total_processed"] or 0,
            "total_success": summary["total_success"] or 0,
            "total_errors": summary["total_errors"] or 0,
            "error_count": summary["error_count"] or 0,
            "warning_count": summary["warning_count"] or 0,
            "step_breakdown": [
                {
                    "step": row["process_step"],
                    "records": row["records"],
                    "log_entries": row["log_entries"]
                }
                for row in step_breakdown
            ]
        }
    
    def get_daily_statistics(self, days_back: int = 30) -> DataFrame:
        """
        Get daily ETL statistics.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with daily statistics
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        return self.spark.read \
            .format("delta") \
            .load(self.log_table_path) \
            .filter(F.col("execution_date") >= cutoff_date) \
            .groupBy("execution_date") \
            .agg(
                F.countDistinct("etl_run_id").alias("total_runs"),
                F.sum("records_processed").alias("total_records"),
                F.sum("records_success").alias("successful_records"),
                F.sum("records_error").alias("error_records"),
                F.count(F.when(F.col("status") == "E", 1)).alias("error_logs"),
                F.count(F.when(F.col("status") == "W", 1)).alias("warning_logs")
            ) \
            .orderBy("execution_date")
    
    def cleanup_old_logs(self, retention_days: int = 90) -> int:
        """
        Clean up logs older than retention period.
        
        Args:
            retention_days: Number of days to retain logs
            
        Returns:
            Number of records deleted
        """
        cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        
        # Read current logs
        logs_df = self.spark.read \
            .format("delta") \
            .load(self.log_table_path)
        
        # Count records to delete
        delete_count = logs_df.filter(F.col("execution_date") < cutoff_date).count()
        
        # Keep only recent logs
        recent_logs = logs_df.filter(F.col("execution_date") >= cutoff_date)
        
        # Overwrite table
        recent_logs.write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.log_table_path)
        
        return delete_count