"""
ETL Logger Module

Generates unique log IDs and persists log entries to database.
"""

import time
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession


class ETLLogger:
    """
    Logger class that generates unique log IDs (LOG + 14-digit timestamp)
    and inserts log entries to ZETL_LOG table.
    """

    def __init__(self, spark: SparkSession, etl_run_id: str, config: Dict[str, Any]):
        """
        Initialize ETL Logger.

        Args:
            spark: SparkSession instance
            etl_run_id: Unique identifier for the ETL run
            config: Configuration dictionary containing database settings
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.db_config = config.get('database', {})

    def generate_log_id(self) -> str:
        """
        Generate unique log ID based on timestamp.

        Returns:
            str: Unique log ID in format LOG + 14-digit timestamp
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        # Add microseconds for additional uniqueness
        microseconds = str(int(time.time() * 1000000))[-2:]
        return f"LOG{timestamp}{microseconds}"

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
        Log a message with execution metadata to database.

        Args:
            step: Process step (e.g., 'EXTRACT', 'TRANSFORM', 'LOAD')
            status: Status code ('S', 'E', 'W', 'I')
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_id = self.generate_log_id()
        execution_date = datetime.now().strftime('%Y-%m-%d')
        execution_time = datetime.now().strftime('%H:%M:%S')
        created_at = datetime.now().isoformat()
        created_by = self.config.get('user', 'etl_system')

        log_entry = {
            'log_id': log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': execution_date,
            'execution_time': execution_time,
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message[:255],  # Truncate to 255 chars
            'created_at': created_at,
            'created_by': created_by
        }

        # Console output
        print(f"[{execution_time}] [{step}] [{status}] {message}")

        # Insert to database
        try:
            self._insert_log_entry(log_entry)
        except Exception as e:
            print(f"WARNING: Failed to insert log entry to database: {str(e)}")
            # Continue execution even if logging fails

    def _insert_log_entry(self, log_entry: Dict[str, Any]) -> None:
        """
        Insert log entry to ZETL_LOG table using appropriate database connector.

        Args:
            log_entry: Dictionary containing log entry data
        """
        db_type = self.db_config.get('type', 'jdbc')

        if db_type == 'jdbc':
            self._insert_via_jdbc(log_entry)
        elif db_type == 'hana':
            self._insert_via_hana(log_entry)
        elif db_type == 'delta':
            self._insert_via_delta(log_entry)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def _insert_via_jdbc(self, log_entry: Dict[str, Any]) -> None:
        """Insert log entry via JDBC connector."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        schema = StructType([
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

        df = self.spark.createDataFrame([log_entry], schema=schema)

        jdbc_url = self.db_config.get('jdbc_url')
        table_name = self.db_config.get('log_table', 'ZETL_LOG')
        properties = {
            "user": self.db_config.get('user', ''),
            "password": self.db_config.get('password', ''),
            "driver": self.db_config.get('driver', 'com.sap.db.jdbc.Driver')
        }

        df.write \
            .jdbc(url=jdbc_url, table=table_name, mode='append', properties=properties)

    def _insert_via_hana(self, log_entry: Dict[str, Any]) -> None:
        """Insert log entry via SAP HANA connector."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        schema = StructType([
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

        df = self.spark.createDataFrame([log_entry], schema=schema)

        table_name = self.db_config.get('log_table', 'ZETL_LOG')

        df.write \
            .format("com.sap.spark.hana") \
            .option("url", self.db_config.get('hana_url')) \
            .option("user", self.db_config.get('user')) \
            .option("password", self.db_config.get('password')) \
            .option("table", table_name) \
            .mode("append") \
            .save()

    def _insert_via_delta(self, log_entry: Dict[str, Any]) -> None:
        """Insert log entry via Delta Lake."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        schema = StructType([
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

        df = self.spark.createDataFrame([log_entry], schema=schema)

        delta_path = self.db_config.get('delta_log_path', '/mnt/delta/zetl_log')

        df.write \
            .format("delta") \
            .mode("append") \
            .save(delta_path)

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.

        Returns:
            str: ETL run ID
        """
        return self.etl_run_id


def generate_etl_run_id() -> str:
    """
    Generate unique ETL run ID.

    Returns:
        str: Unique ETL run ID in format ETL + 14-digit timestamp
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    microseconds = str(int(time.time() * 1000000))[-2:]
    return f"ETL{timestamp}{microseconds}"