"""
ETL Logging Module
Provides logging functionality for ETL process
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """Logger for ETL process execution"""
    
    def __init__(self, spark: SparkSession, config: dict, etl_run_id: str):
        """
        Initialize the logger
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.etl_run_id = etl_run_id
        self._setup_logging()
        self.log_entries = []
    
    def _setup_logging(self) -> None:
        """Setup Python logging"""
        log_config = self.config['logging']
        
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format=log_config['format']
        )
        
        self.logger = logging.getLogger(__name__)
        
        if log_config['file']['enabled']:
            file_handler = logging.FileHandler(log_config['file']['path'])
            file_handler.setFormatter(
                logging.Formatter(log_config['format'])
            )
            self.logger.addHandler(file_handler)
    
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
        Log a message
        
        Args:
            step: ETL step name
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().date().isoformat(),
            "execution_time": datetime.now().time().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "created_at": datetime.now().isoformat()
        }
        
        self.log_entries.append(log_entry)
        
        # Also log to Python logger
        log_level = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }.get(status, logging.INFO)
        
        self.logger.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id
    
    def flush_logs_to_database(self) -> None:
        """Write accumulated logs to database"""
        if not self.log_entries:
            return
        
        try:
            db_config = self.config['database']['log']
            
            log_df = self.spark.createDataFrame(self.log_entries)
            
            log_df.write \
                .format(db_config['format']) \
                .option("url", db_config['url']) \
                .option("dbtable", db_config['table']) \
                .option("driver", db_config['driver']) \
                .option("user", db_config['user']) \
                .option("password", db_config['password']) \
                .mode("append") \
                .save()
            
            self.logger.info(f"Flushed {len(self.log_entries)} log entries to database")
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs to database: {str(e)}")
    
    def get_log_summary(self) -> dict:
        """Get summary of log entries"""
        summary = {
            "total_entries": len(self.log_entries),
            "errors": sum(1 for log in self.log_entries if log['status'] == 'E'),
            "warnings": sum(1 for log in self.log_entries if log['status'] == 'W'),
            "success": sum(1 for log in self.log_entries if log['status'] == 'S')
        }
        return summary