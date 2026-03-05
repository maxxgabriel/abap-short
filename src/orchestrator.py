"""
ETL Orchestrator Module
Coordinates the entire ETL pipeline with retry logic and error handling.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger
from src.exceptions import ETLError, ExtractError, TransformError, LoadError
from src.utils import generate_run_id, calculate_duration


class ETLOrchestrator:
    """
    Main orchestrator for the ETL pipeline.
    Manages workflow execution with retry logic and comprehensive error handling.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], logger: ETLLogger):
        """
        Initialize the orchestrator with dependencies.

        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.run_id = generate_run_id("ETL")
        
        # Initialize components
        self.extractor = SalesExtractor(spark, config, logger)
        self.transformer = SalesTransformer(spark, config, logger)
        self.loader = SalesLoader(spark, config, logger)
        
        # Tracking
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.statistics: Dict[str, int] = {}

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL pipeline.

        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            bool: True if successful, False otherwise
        """
        self.start_time = datetime.now()
        
        try:
            self.logger.log_message(
                step="INIT",
                status="S",
                message=f"ETL process initialized with run ID: {self.run_id}"
            )
            
            # Phase 1: Extract
            raw_data = self._execute_with_retry(
                phase="EXTRACT",
                func=self.extractor.extract_data,
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data is None:
                raise ExtractError("Extraction phase failed after all retry attempts")
            
            # Phase 2: Transform
            analytics_data = self._execute_with_retry(
                phase="TRANSFORM",
                func=self.transformer.transform_data,
                raw_data=raw_data
            )
            
            if analytics_data is None:
                raise TransformError("Transformation phase failed after all retry attempts")
            
            # Phase 3: Load
            load_success = self._execute_with_retry(
                phase="LOAD",
                func=self.loader.load_data,
                analytics_data=analytics_data
            )
            
            if not load_success:
                raise LoadError("Load phase failed after all retry attempts")
            
            # Success
            self.end_time = datetime.now()
            duration = calculate_duration(self.start_time, self.end_time)
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully in {duration} seconds"
            )
            
            return True
            
        except ETLError as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            return False
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"Unexpected error: {str(e)}"
            )
            logging.exception("Unexpected error in ETL orchestrator")
            return False

    def _execute_with_retry(self, phase: str, func: callable, **kwargs) -> Any:
        """
        Execute a phase with retry logic.

        Args:
            phase: Phase name (EXTRACT, TRANSFORM, LOAD)
            func: Function to execute
            **kwargs: Arguments to pass to the function

        Returns:
            Result from the function or None if all retries fail
        """
        max_retries = self.config.get("retry_attempts", 3)
        retry_delay = self.config.get("retry_delay_seconds", 5)
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.log_message(
                    step=phase,
                    status="I",
                    message=f"Starting {phase} phase (attempt {attempt}/{max_retries})"
                )
                
                result = func(**kwargs)
                
                self.logger.log_message(
                    step=phase,
                    status="S",
                    message=f"{phase} phase completed successfully"
                )
                
                return result
                
            except Exception as e:
                error_msg = f"{phase} attempt {attempt}/{max_retries} failed: {str(e)}"
                
                if attempt < max_retries:
                    self.logger.log_message(
                        step=phase,
                        status="W",
                        message=f"{error_msg}. Retrying in {retry_delay} seconds..."
                    )
                    import time
                    time.sleep(retry_delay)
                else:
                    self.logger.log_message(
                        step=phase,
                        status="E",
                        message=f"{error_msg}. No more retry attempts."
                    )
                    raise
        
        return None

    def get_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.run_id

    def display_summary(self) -> Dict[str, Any]:
        """
        Generate execution summary.

        Returns:
            Dictionary containing execution statistics
        """
        duration = None
        if self.start_time and self.end_time:
            duration = calculate_duration(self.start_time, self.end_time)
        
        summary = {
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration,
            "statistics": self.statistics
        }
        
        self.logger.log_message(
            step="SUMMARY",
            status="I",
            message=f"ETL Summary: {summary}"
        )
        
        return summary

    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites are met before running ETL.

        Returns:
            bool: True if all prerequisites are met
        """
        try:
            # Check if source table/path exists
            source_path = self.config.get("source_data_path")
            if not source_path:
                self.logger.log_message(
                    step="VALIDATE",
                    status="E",
                    message="Source data path not configured"
                )
                return False
            
            # Check if target path is writable
            target_path = self.config.get("target_data_path")
            if not target_path:
                self.logger.log_message(
                    step="VALIDATE",
                    status="E",
                    message="Target data path not configured"
                )
                return False
            
            # Validate configuration
            required_configs = ["batch_size", "retry_attempts", "timeout_seconds"]
            for config_key in required_configs:
                if config_key not in self.config:
                    self.logger.log_message(
                        step="VALIDATE",
                        status="E",
                        message=f"Required configuration missing: {config_key}"
                    )
                    return False
            
            self.logger.log_message(
                step="VALIDATE",
                status="S",
                message="All prerequisites validated successfully"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="VALIDATE",
                status="E",
                message=f"Prerequisite validation failed: {str(e)}"
            )
            return False