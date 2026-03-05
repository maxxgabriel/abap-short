"""
Factory for creating ETL logger instances
"""
from typing import Optional
from pyspark.sql import SparkSession

from src.logger_interface import ETLLoggerInterface
from src.logger import ETLLogger


class LoggerFactory:
    """
    Factory class for creating logger instances.
    Provides centralized logger creation with consistent configuration.
    """

    @staticmethod
    def create_logger(
        etl_run_id: str,
        spark: Optional[SparkSession] = None,
        config: Optional[dict] = None
    ) -> ETLLoggerInterface:
        """
        Create an ETL logger instance.
        
        Args:
            etl_run_id: Unique ETL run identifier
            spark: Optional SparkSession for database logging
            config: Optional configuration dictionary
            
        Returns:
            ETLLoggerInterface implementation
        """
        config = config or {}
        
        log_table = config.get('log_table')
        console_level = config.get('console_level', 'INFO')
        
        # Map string level to logging constant
        import logging
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        console_level_int = level_map.get(console_level.upper(), logging.INFO)
        
        return ETLLogger(
            etl_run_id=etl_run_id,
            spark=spark,
            log_table=log_table,
            console_level=console_level_int
        )

    @staticmethod
    def generate_etl_run_id() -> str:
        """
        Generate a unique ETL run ID.
        
        Returns:
            Unique ETL run ID (format: ETL<timestamp>)
        """
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"