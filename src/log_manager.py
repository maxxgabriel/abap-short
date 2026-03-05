"""
Log Manager for advanced log operations
Provides utilities for log maintenance, querying, and analysis
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable
import logging


class LogManager:
    """
    Advanced log management utilities for ETL logging system.
    Provides operations for log maintenance, analysis, and optimization.
    """
    
    def __init__(self, spark: SparkSession, delta_table_path: str):
        """
        Initialize Log Manager.
        
        Args:
            spark: SparkSession instance
            delta_table_path: Path to Delta table containing logs
        """
        self.spark = spark
        self.delta_table_path = delta_table_path
        self.logger = logging.getLogger(__name__)
    
    def compact_logs(self) -> None:
        """
        Compact Delta table to optimize storage and query performance.
        Runs OPTIMIZE and VACUUM operations.
        """
        try:
            self.logger.info("Starting log compaction...")
            
            # Optimize Delta table
            delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
            delta_table.optimize().executeCompaction()
            
            self.logger.info("Log compaction completed")
            
        except Exception as e:
            self.logger.error(f"Failed to compact logs: {e}")
            raise
    
    def vacuum_old_logs(self, retention_days: int = 90) -> None:
        """
        Remove old log files based on retention policy.
        
        Args:
            retention_days: Number of days to retain logs
        """
        try:
            self.logger.info(f"Vacuuming logs older than {retention_days} days...")
            
            delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
            delta_table.vacuum(retention_days * 24)  # Convert days to hours
            
            self.logger.info("Vacuum completed")
            
        except Exception as e:
            self.logger.error(f"Failed to vacuum logs: {e}")
            raise
    
    def get_error_summary(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> DataFrame:
        """
        Get summary of errors grouped by process step.
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            
        Returns:
            DataFrame with error summary
        """
        df = self.spark.read.format("delta").load(self.delta_table_path)
        df = df.filter(F.col("status") == "E")
        
        if start_date:
            df = df.filter(F.col("execution_date") >= start_date.date())
        if end_date:
            df = df.filter(F.col("execution_date") <= end_date.date())
        
        summary = df.groupBy("process_step", "message").agg(
            F.count("*").alias("error_count"),
            F.min("execution_time").alias("first_occurrence"),
            F.max("execution_time").alias("last_occurrence"),
            F.sum("records_error").alias("total_records_affected")
        ).orderBy(F.desc("error_count"))
        
        return summary
    
    def get_performance_metrics(
        self,
        etl_run_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get performance metrics for ETL runs.
        
        Args:
            etl_run_id: Optional specific ETL run ID
            
        Returns:
            Dictionary with performance metrics
        """
        df = self.spark.read.format("delta").load(self.delta_table_path)
        
        if etl_run_id:
            df = df.filter(F.col("etl_run_id") == etl_run_id)
        
        # Calculate metrics per step
        step_metrics = df.groupBy("etl_run_id", "process_step").agg(
            F.min("execution_time").alias("step_start"),
            F.max("execution_time").alias("step_end"),
            F.sum("records_processed").alias("records_processed"),
            F.sum("records_success").alias("records_success"),
            F.sum("records_error").alias("records_error")
        )
        
        # Calculate duration for each step
        step_metrics = step_metrics.withColumn(
            "duration_seconds",
            (F.col("step_end").cast("long") - F.col("step_start").cast("long"))
        )
        
        # Calculate throughput
        step_metrics = step_metrics.withColumn(
            "throughput_per_second",
            F.when(
                F.col("duration_seconds") > 0,
                F.col("records_processed") / F.col("duration_seconds")
            ).otherwise(0)
        )
        
        return step_metrics
    
    def archive_old_logs(
        self,
        archive_path: str,
        cutoff_date: datetime,
        delete_after_archive: bool = False
    ) -> int:
        """
        Archive old logs to a different location.
        
        Args:
            archive_path: Path to archive location
            cutoff_date: Archive logs older than this date
            delete_after_archive: Whether to delete logs after archiving
            
        Returns:
            Number of archived records
        """
        try:
            self.logger.info(f"Archiving logs older than {cutoff_date}...")
            
            df = self.spark.read.format("delta").load(self.delta_table_path)
            old_logs = df.filter(F.col("execution_date") < cutoff_date.date())
            
            count = old_logs.count()
            
            if count > 0:
                # Write to archive
                old_logs.write.format("delta").mode("append").save(archive_path)
                
                if delete_after_archive:
                    # Delete archived records from main table
                    delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)
                    delta_table.delete(F.col("execution_date") < cutoff_date.date())
                
                self.logger.info(f"Archived {count} log records")
            else:
                self.logger.info("No logs to archive")
            
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to archive logs: {e}")
            raise
    
    def get_etl_run_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> DataFrame:
        """
        Get aggregated statistics for ETL runs.
        
        Args:
            start_date: Filter start date
            end_date: Filter end date
            
        Returns:
            DataFrame with ETL run statistics
        """
        df = self.spark.read.format("delta").load(self.delta_table_path)
        
        if start_date:
            df = df.filter(F.col("execution_date") >= start_date.date())
        if end_date:
            df = df.filter(F.col("execution_date") <= end_date.date())
        
        stats = df.groupBy("etl_run_id").agg(
            F.min("execution_date").alias("run_date"),
            F.min("execution_time").alias("start_time"),
            F.max("execution_time").alias("end_time"),
            F.sum("records_processed").alias("total_processed"),
            F.sum("records_success").alias("total_success"),
            F.sum("records_error").alias("total_error"),
            F.sum(F.when(F.col("status") == "E", 1).otherwise(0)).alias("error_count"),
            F.sum(F.when(F.col("status") == "W", 1).otherwise(0)).alias("warning_count"),
            F.count("*").alias("log_entries")
        )
        
        # Calculate duration
        stats = stats.withColumn(
            "duration_seconds",
            (F.col("end_time").cast("long") - F.col("start_time").cast("long"))
        )
        
        # Calculate success rate
        stats = stats.withColumn(
            "success_rate",
            F.when(
                F.col("total_processed") > 0,
                (F.col("total_success") / F.col("total_processed")) * 100
            ).otherwise(0)
        )
        
        return stats.orderBy(F.desc("run_date"))
    
    def export_logs_to_json(
        self,
        output_path: str,
        etl_run_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> None:
        """
        Export logs to JSON format for external analysis.
        
        Args:
            output_path: Path to write JSON files
            etl_run_id: Optional ETL run ID filter
            start_date: Optional start date filter
            end_date: Optional end date filter
        """
        try:
            df = self.spark.read.format("delta").load(self.delta_table_path)
            
            if etl_run_id:
                df = df.filter(F.col("etl_run_id") == etl_run_id)
            if start_date:
                df = df.filter(F.col("execution_date") >= start_date.date())
            if end_date:
                df = df.filter(F.col("execution_date") <= end_date.date())
            
            df.write.mode("overwrite").json(output_path)
            
            self.logger.info(f"Exported logs to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export logs: {e}")
            raise