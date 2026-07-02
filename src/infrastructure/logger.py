```python
import logging
import sys
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

class ETLLogger:
    def __init__(self, run_id: str, spark: Optional[SparkSession] = None):
        self.run_id = run_id
        self.spark = spark
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        logger = logging.getLogger(f'ETL_{self.run_id}')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        return logger
    
    def log_message(self, step: str, status: str, message: str,
                   records_processed: int = 0, records_success: int = 0,
                   records_failed: int = 0):
        log_entry = {
            'run_id': self.run_id,
            'step': step,
            'status': status,
            'message': message,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_failed': records_failed,
            'timestamp': datetime.now()
        }
        
        log_msg = f"[{step}] {status}: {message}"
        if records_processed > 0:
            log_msg += f" (Processed: {records_processed}, Success: {records_success}, Failed: {records_failed})"
        
        if status == 'E':
            self.logger.error(log_msg)
        elif status == 'W':
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
        
        if self.spark:
            self._write_to_database(log_entry)
    
    def _write_to_database(self, log_entry: dict):
        try:
            from pyspark.sql import Row
            log_row = Row(**log_entry)
            df = self.spark.createDataFrame([log_row])
            # In production, write to ZETL_LOG table
            # df.write.jdbc(url, table="zetl_log", mode="append", properties=props)
        except Exception as e:
            self.logger.error(f"Failed to write log to database: {e}")
    
    def get_run_id(self) -> str:
        return self.run_id
```